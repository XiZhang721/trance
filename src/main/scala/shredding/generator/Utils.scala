package shredding.generator

import java.io._
import shredding.core._
import shredding.wmcc._
import shredding.examples.Query
import shredding.examples.tpch._

object Utils {

  val normalizer = new Finalizer(new BaseNormalizer{})
  def pathfun(outf: String, sub: String = ""): String = s"experiments/tpch/$sub/test/$outf.scala"
  //val pathout = (outf: String) => s"src/test/scala/shredding/examples/simple/$outf.scala"

  /**
    * Produces an output file for a query pipeline
    * (either shredded or not shredded) the does not do unnesting
    */

  def runCalc(qInfo: (CExpr, String, String), inputM: Map[Type, String], 
              q2Info: (CExpr, String, String) = (EmptySng, "", "")): Unit = {
    val anfBase = new BaseANF {}
    val anfer = new Finalizer(anfBase)

    val (q1, qname, qdata) = qInfo
    
    val normq1 = normalizer.finalize(q1).asInstanceOf[CExpr]
    val inputs = normq1 match {
                   case l @ LinearCSet(_) => inputM ++ l.getTypeMap
                   case _ => inputM ++ Map(normq1.tp.asInstanceOf[BagCType].tp -> s"${qname}Out")
                 }
    val ng = inputM.toList.map(f => f._2)
    val codegen = new ScalaNamedGenerator(inputs)
    
    val anfedq1 = new Finalizer(anfBase).finalize(normq1.asInstanceOf[CExpr])
    val anfExp1 = anfBase.anf(anfedq1.asInstanceOf[anfBase.Rep])
    val gcode = codegen.generate(anfExp1)
    val header = codegen.generateHeader(ng)

    val printer = new PrintWriter(new FileOutputStream(new File(pathfun(qname+"Calc")), false))
    val finalc = write(qname+"Calc", qdata, header, gcode)
    printer.println(finalc)
    printer.close 

    // generate the down stream query
    if (q2Info != (EmptySng, "", "")){
      
      val (q2, q2name, q2data) = q2Info

      val normq2 = normalizer.finalize(q2).asInstanceOf[CExpr]
      anfBase.reset
      val anfedq2 = anfer.finalize(normq2)
      val anfExp2 = anfBase.anf(anfedq2.asInstanceOf[anfBase.Rep])

      val gcode2 = codegen.generate(anfExp2)
      val header2 = codegen.generateHeader(ng)

      val printer2 = new PrintWriter(new FileOutputStream(new File(pathfun(q2name+"Calc")), false))
      val finalc2 = write2(q2name+"Calc", qdata, header2, gcode, q2name, gcode2, q2data)
      printer2.println(finalc2)
      printer2.close 

    }

  }

  /**
    * Produces an output file for a query pipeline 
    * (either shredded or not shredded) that does unnesting
    */
 
  def run(qInfo: (CExpr, String, String), inputM: Map[Type, String], 
          q2Info: (CExpr, String, String) = (EmptySng, "", "")): Unit = {
    val anfBase = new BaseANF {}
    val anfer = new Finalizer(anfBase)

    val (q1, qname, qdata) = qInfo
    
    val normq1 = normalizer.finalize(q1).asInstanceOf[CExpr]
    val inputs = normq1 match {
                  case l @ LinearCSet(_) => inputM ++ l.getTypeMap
                  case _ => inputM ++ Map(normq1.tp.asInstanceOf[BagCType].tp -> s"${qname}Out")
                 }
    val ng = inputM.toList.map(f => f._2)
    val codegen = new ScalaNamedGenerator(inputs)
    
    val plan1 = Unnester.unnest(normq1)(Nil, Nil, None).asInstanceOf[CExpr]
    println(Printer.quote(plan1))
    val anfedq1 = anfer.finalize(plan1)
    val anfExp1 = anfBase.anf(anfedq1.asInstanceOf[anfBase.Rep])
    println(Printer.quote(anfExp1))
    val gcode = codegen.generate(anfExp1)
    val header = codegen.generateHeader(ng)

    val printer = new PrintWriter(new FileOutputStream(new File(pathfun(qname)), false))
    val finalc = write(qname, qdata, header, gcode)
    printer.println(finalc)
    printer.close 

    // generate the down stream query
    if (q2Info != (EmptySng, "", "")){
      val (q2, q2name, q2data) = q2Info

      val normq2 = normalizer.finalize(q2).asInstanceOf[CExpr]
      println(Printer.quote(normq2))
      val plan2 = Unnester.unnest(normq2)(Nil, Nil, None)
      println(Printer.quote(plan2))
      anfBase.reset
      val anfedq2 = anfer.finalize(plan2)
      val anfExp2 = anfBase.anf(anfedq2.asInstanceOf[anfBase.Rep])

      val gcode2 = codegen.generate(anfExp2)
      val header2 = codegen.generateHeader(ng)

      val printer2 = new PrintWriter(new FileOutputStream(new File(pathfun(q2name)), false))
      val finalc2 = write2(q2name, qdata, header2, gcode, q2name, gcode2, q2data)
      printer2.println(finalc2)
      printer2.close 
    }

  }
 
   /**
    * Produces an ouptut spark application 
    * (either shredded or not shredded) that does unnesting
    */
  def timeOp(appname: String, e: String, i: Int = 0): String = {
    val query = if (i > 0) "unshredding" else "query"
    s"""
      |var start$i = System.currentTimeMillis()
      |$e
      |var end$i = System.currentTimeMillis() - start$i
      |println("$appname,"+sf+","+Config.datapath+","+end$i+",$query,"+spark.sparkContext.applicationId)
    """.stripMargin
  }

  def timed(appname: String, e: List[String]): String =
    s"""| def f = {
        | ${e.zipWithIndex.map{ case (e1,i) => timeOp(appname, e1, i) }.mkString("\n")}
        |}
        |var start = System.currentTimeMillis()
        |f
        |var end = System.currentTimeMillis() - start
    """.stripMargin
   
  def timed(e: String, shred: Boolean = true): String = 
    s"""|def f = { 
        | ${e}${if (!shred) ".evaluate"}
        |}
        |var start = System.currentTimeMillis()
        |f
        |var end = System.currentTimeMillis() - start """.stripMargin

  
  def flat(query: Query, path: String, label: String): Unit =
    runSparkNoDomains(query, path, label, 0, false, false)

  def flatInput(input: Query, query: Query, path: String, label: String): Unit =
    runSparkInputNoDomains(input, query, path, label, 0, shred = false)

  def flatProj(query: Query, path: String, label: String): Unit =
    runSparkNoDomains(query, path, label, 1, false, false)

  def flatProjInput(input: Query, query: Query, path: String, label: String): Unit =
    runSparkInputNoDomains(input, query, path, label, 1, shred = false)

  def flatOpt(query: Query, path: String, label: String, skew: Boolean = false): Unit =
    runSparkNoDomains(query, path, label, 2, false, skew)

  def flatOptInput(input: Query, query: Query, path: String, label: String, skew: Boolean = false): Unit =
    runSparkInputNoDomains(input, query, path, label, 2, shred = false, skew = skew)

  def shredDomains(query: Query, path: String, label: String, skew: Boolean = false): Unit =
    runSparkDomains(query, path, label, false, skew)

  def shredDomainsInput(input: Query, query: Query, path: String, label: String, skew: Boolean = false): Unit =
    runSparkInputDomains(input, query, path, label, unshred = false, skew = skew)

  def unshredDomains(query: Query, path: String, label: String, skew: Boolean = false): Unit =
    runSparkDomains(query, path, label, true, skew)

  def unshredDomainsInput(input: Query, query: Query, path: String, label: String, skew: Boolean = false): Unit =
    runSparkInputDomains(input, query, path, label, unshred = true, skew = skew)


  def runSparkNoDomains(query: Query, pathout: String, label: String, optLevel: Int = 2, 
    shred: Boolean = false, skew: Boolean = false): Unit = {
    
    val codegen = new SparkNamedGenerator(query.inputTypes(shred))
    val gcode = if (shred) codegen.generate(query.sanf) else codegen.generate(query.anf(optLevel))
    val header = if (skew) {
        s"""|import sprkloader.SkewPairRDD._
            ${if (shred) "|import sprkload.SkewDictRDD._" else ""}
            |import sprkloader.SkewTopRDD._
            |${codegen.generateHeader(query.headerTypes(shred))}""".stripMargin
      } else {
        s"""|import sprkloader.PairRDDOperations._
            ${if (shred) "|import sprkload.DictRDDOperations._" else ""}
            |import sprkloader.TopRDD._
            |${codegen.generateHeader(query.headerTypes(shred))}""".stripMargin
      }
   
    val flatTag = optLevel match {
      case 0 => "None"
      case 1 => "Proj"
      case _ => ""
    }
    val qname1 = if (shred) s"Shred${query.name}Spark" else s"${query.name}${flatTag}Spark"
    val qname = if (skew) s"${qname1}Skew" else qname1
    val fname = s"$pathout/$qname.scala" 
    println(s"Writing out $qname to $fname")
    val printer = new PrintWriter(new FileOutputStream(new File(fname), false))
    val inputs = if (skew) query.inputs(if (shred) TPCHSchema.sskewcmds else TPCHSchema.skewcmds)
      else query.inputs(if (shred) TPCHSchema.stblcmds else TPCHSchema.tblcmds)
    val finalc = writeSpark(qname, inputs, header, timed(gcode, shred), label)
    printer.println(finalc)
    printer.close 
  
  }

  def runSparkInputNoDomains(inputQuery: Query, query: Query, pathout: String, label: String, optLevel: Int = 2, 
    shred: Boolean = false, skew: Boolean = false): Unit = {
    
    val codegen = new SparkNamedGenerator(inputQuery.inputTypes(shred))
    val (inputCode, gcode) = 
      if (shred) (codegen.generate(inputQuery.sanf), codegen.generate(query.sanf))
      else (codegen.generate(inputQuery.anf()), codegen.generate(query.anf(optLevel)))
    val header = if (skew) {
        s"""|import sprkloader.SkewPairRDD._
            ${if (shred) "|import sprkload.SkewDictRDD._" else ""}
            |import sprkloader.SkewTopRDD._
            |${codegen.generateHeader(query.headerTypes(shred))}""".stripMargin
      } else {
        s"""|import sprkloader.PairRDDOperations._
            ${if (shred) "|import sprkload.DictRDDOperations._" else ""}
            |import sprkloader.TopRDD._
            |${codegen.generateHeader(query.headerTypes(shred))}""".stripMargin
      }
    val flatTag = optLevel match {
      case 0 => "None"
      case 1 => "Proj"
      case _ => ""
    }
    val qname1 = if (shred) s"Shred${query.name}Spark" else s"${query.name}${flatTag}Spark"
    val qname = if (skew) s"${qname1}Skew" else qname1
    val fname = s"$pathout/$qname.scala"
    println(s"Writing out $qname to $fname")
    val printer = new PrintWriter(new FileOutputStream(new File(fname), false))
    val inputSection = 
      if (shred) s"${inputCode.split("\n").dropRight(1).mkString("\n")}\n${shredInputs(inputQuery.indexedDict)}"
      else s"${inputs(inputQuery.name, inputCode)}"
    val finalc = writeSpark(qname, query.inputs(if (shred) TPCHSchema.stblcmds else TPCHSchema.tblcmds), 
                  header, s"$inputSection\n${timed(gcode, shred)}", label)
    printer.println(finalc)
    printer.close 
  
  }

  def runSparkInputDomains(inputQuery: Query, query: Query, pathout: String, label: String, 
    unshred: Boolean = false, skew: Boolean = false): Unit = {
    
    val codegen = new SparkNamedGenerator(query.inputTypes(true))
    val inputCode = codegen.generate(inputQuery.shredANF)
    val gcode1 = codegen.generate(query.shredANF)
    val gcodeSet = if (unshred) {
        List(gcode1, codegen.generate(query.unshredANF)) 
      }else List(gcode1)
    val header = if (skew) {
        s"""|import sprkloader.SkewPairRDD._
            |import sprkloader.SkewDictRDD._
            |import sprkloader.SkewTopRDD._
            |${codegen.generateHeader(query.headerTypes(true))}""".stripMargin
      } else {
        s"""|import sprkloader.PairRDDOperations._
            |import sprkloader.DictRDDOperations._
            |import sprkloader.TopRDD._
            |${codegen.generateHeader(query.headerTypes(true))}""".stripMargin
      }
   
    val qname = if (skew) s"Shred${query.name}DomainsSparkSkew" else s"Shred${query.name}DomainsSpark"
    val fname = if (unshred) s"$pathout/unshred/$qname.scala" else s"$pathout/$qname.scala"
    println(s"Writing out $qname to $fname")
    val printer = new PrintWriter(new FileOutputStream(new File(fname), false))
    val inputSection = s"${inputCode.split("\n").dropRight(1).mkString("\n")}\n${shredInputs(inputQuery.indexedDict)}"
    val inputs = if (skew) query.inputs(TPCHSchema.sskewcmds) else query.inputs(TPCHSchema.stblcmds)
    val finalc = writeSpark(qname, inputs, header, s"$inputSection\n${timed(label, gcodeSet)}", label)
    printer.println(finalc)
    printer.close 
  
  }

  def runSparkDomains(query: Query, pathout: String, label: String, unshred: Boolean = false, skew: Boolean = false): Unit = {
    
    val codegen = new SparkNamedGenerator(query.inputTypes(true))
    val gcode1 = codegen.generate(query.shredANF)
    val gcodeSet = if (unshred) {
        List(gcode1, codegen.generate(query.unshredANF)) 
      }else List(gcode1)
    val header = if (skew) {
        s"""|import sprkloader.SkewPairRDD._
            |import sprkloader.SkewDictRDD._
            |import sprkloader.SkewTopRDD._
            |${codegen.generateHeader(query.headerTypes(true))}""".stripMargin
      } else {
        s"""|import sprkloader.PairRDDOperations._
            |import sprkloader.DictRDDOperations._
            |import sprkloader.TopRDD._
            |${codegen.generateHeader(query.headerTypes(true))}""".stripMargin
      }
   
    val qname = if (skew) s"Shred${query.name}DomainsSparkSkew" else s"Shred${query.name}DomainsSpark"
    val fname = if (unshred) s"$pathout/unshred/$qname.scala" else s"$pathout/$qname.scala"
    println(s"Writing out $qname to $fname")
    val printer = new PrintWriter(new FileOutputStream(new File(fname), false))
    val inputs = if (skew) query.inputs(TPCHSchema.sskewcmds) else query.inputs(TPCHSchema.stblcmds)
    val finalc = writeSpark(qname, inputs, header, timed(label, gcodeSet), label)
    printer.println(finalc)
    printer.close 
  
  }

  def inputs(n: String, e: String): String = {
    s"""|val $n = {
        | $e
        |}
        |$n.cache
        |$n.evaluate""".stripMargin
  }
 
  def shredInputs(ns: List[String]): String = { 
    var cnt = 0
    ns.map{ n => 
      cnt += 1
      s"""|val $n = M__D_$cnt
          |$n.cache
          |$n.evaluate"""
    }.mkString("\n").stripMargin
  }

  /**
    * Writes out a query for a Spark application
    **/

  def writeSpark(appname: String, data: String, header: String, gcode: String, label:String): String  = {
    s"""
      |package experiments
      |/** Generated **/
      |import org.apache.spark.SparkConf
      |import org.apache.spark.sql.SparkSession
      |import sprkloader._
      |$header
      |object $appname {
      | def main(args: Array[String]){
      |   val sf = Config.datapath.split("/").last
      |   val conf = new SparkConf().setMaster(Config.master).setAppName(\"$appname\"+sf)
      |   val spark = SparkSession.builder().config(conf).getOrCreate()
      |   $data
      |   $gcode
      |   println("$label,"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
      | }
      |}""".stripMargin
  }

  /**
    * Writes out a query that has inputs provided from the context (h)
    */

  def write(n: String, i: String, h: String, q: String): String = {
    s"""
      |package experiments
      |/** Generated code **/
      |import shredding.core.CaseClassRecord
      |import shredding.examples.tpch._
      |    $h
      |object $n {
      | def main(args: Array[String]){
      |    var start0 = System.currentTimeMillis()
      |    var id = 0L
      |    def newId: Long = {
      |      val prevId = id
      |      id += 1
      |      prevId
      |    }
      |    $i
      |    var end0 = System.currentTimeMillis() - start0
      |    def f(){
      |      $q
      |    }
      |    var time = List[Long]()
      |    for (i <- 1 to 5) {
      |      var start = System.currentTimeMillis()
      |      f
      |      var end = System.currentTimeMillis() - start
      |      time = time :+ end
      |    }
      |    val avg = (time.sum/5)
      |    println(end0+","+avg)
      | }
      |}""".stripMargin
  }

  /**
    * Writes out a query that takes another query as input
    */
  
  def write2(n: String, i1: String, h: String, q1: String, i2: String, q2: String, ef: String = ""): String = {
    s"""
      |package experiments
      |/** Generated code **/
      |import shredding.core.CaseClassRecord
      |import shredding.examples.tpch._
      |$h
      |object $n {
      | def main(args: Array[String]){
      |    var start0 = System.currentTimeMillis()
      |    var id = 0L
      |    def newId: Long = {
      |      val prevId = id
      |      id += 1
      |      prevId
      |    }
      |    $i1
      |    val $i2 = { $q1 }
      |    var end0 = System.currentTimeMillis() - start0
      |    $ef
      |    def f(){
      |      $q2
      |    }
      |    var time = List[Long]()
      |    for (i <- 1 to 5) {
      |     var start = System.currentTimeMillis()
      |      f
      |      var end = System.currentTimeMillis() - start
      |      time = time :+ end
      |    }
      |    val avg = (time.sum/5)
      |    println(end0+","+avg)
      | }
      |}""".stripMargin
  }

}
