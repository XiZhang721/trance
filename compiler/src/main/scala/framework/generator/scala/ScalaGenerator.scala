package framework.generator.scala

import framework.core._
import framework.plans._
import framework.utils.Utils.ind

/**
  * Generates Scala code, assumes nodes are compiled from ANF
  * 
  * This was used for exploring local evaluation. 
  * @deprecated This has not been updated while the pipeline has 
  * been evolving while developing the Spark code generator. 
  * 
  * @param inputs map of types and names to avoid making case class 
  * records for when creating the header.
  */

class ScalaNamedGenerator(inputs: Map[Type, String] = Map()) {
 
  var types: Map[Type, String] = inputs
  var typelst: Seq[Type] = Seq()

  implicit def expToString(e: CExpr): String = generate(e)
  
  /** Translate a k,v record into a tuple type
    * 
    * @param x string attribute name
    */
  private def kvName(x: String): String = x match {
    case "k" => "_1"
    case "v" => "_2" 
    case _ => x
  } 

  /** Generator for named tuples, used when creating the 
    * header.
    * 
    * @param tp type of record
    * @return string representing code to create a class class
    */
  private def generateTypeDef(tp: Type): String = tp match {
    case RecordCType(fs) =>
     val name = types(tp)
      s"case class $name(${fs.map(x => s"${kvName(x._1)}: ${generateType(x._2)}").mkString(", ")}, uniqueId: Long) extends CaseClassRecord" 
    case _ => sys.error("unsupported type "+tp)
  }

  /** Generator for all types, used within the main generator.
    *
    * @param tp type of expression from within generator
    * @return string representing the native Scala type
    */
  private def generateType(tp: Type): String = tp match {
    case RecordCType(_) if types.contains(tp) => types(tp)
    case IntType => "Int"
    case StringType => "String"
    case BoolType => "Boolean"
    case DoubleType => "Double"
    case TTupleType(fs) => s"${fs.map(generateType(_)).mkString(",")})"
    case BagCType(tp) => s"List[${generateType(tp)}]" 
    case BagDictCType(flat @ BagCType(TTupleType(fs)), dict) => 
      dict match {
        case TupleDictCType(ds) if !ds.filter(_._2 != EmptyDictCType).isEmpty => 
          s"(List[(${generateType(fs.head)}, ${generateType(fs.last)})], ${generateType(dict)})"
        case _ => s"(List[(${generateType(fs.head)}, ${generateType(fs.last)})], Unit)"
      }
    case TupleDictCType(fs) if !fs.filter(_._2 != EmptyDictCType).isEmpty => 
      generateType(RecordCType(fs.filter(_._2 != EmptyDictCType)))
    case LabelType(fs) if fs.isEmpty => "Int" 
    case LabelType(fs) => generateType(RecordCType(fs))
    case _ => sys.error("not supported type " + tp)
  }

  /** Generates the code for the set of case class records associated to the 
    * records in the generated program.
    *
    * @param names list of names to omit from header creation
    * @return string representing all the records required to run the corresponding application
    */
  def generateHeader(names: List[String] = List()): String = {
    val h1 = typelst.map(x => generateTypeDef(x)).mkString("\n")
    val h2 = inputs.withFilter(x => !names.contains(x._2)).map( x => generateTypeDef(x._1)).toList
    if (h2.nonEmpty) { s"$h1\n${h2.mkString("\n")}" } else { h1 }
  }

  /** Updates the type map with records and record attribute types 
    * from the generated plan.
    *
    * @param tp type from the input program (called for records)
    * @param givenName optional name used for naming the generated record
    */
  private def handleType(tp: Type, givenName: Option[String] = None): Unit = {
    if(!types.contains(tp)) {
      tp match {
        case RecordCType(fs) =>
          fs.foreach(f => handleType(f._2))
          val name = givenName.getOrElse("Record" + Variable.newId)
          types = types + (tp -> name)
          typelst = typelst :+ tp 
        case BagCType(tp) =>
          handleType(tp, givenName)
        case LabelType(fs) if !fs.isEmpty => 
          val name = givenName.getOrElse("Label" + Variable.newId)
          handleType(RecordCType(fs), Some(name))
        case BagDictCType(flat @ BagCType(TTupleType(fs)), dict) =>
          val nid = Variable.newId 
          handleType(fs.head, Some(givenName.getOrElse("")+s"Label$nid"))
          handleType(fs.last, Some(givenName.getOrElse("")+s"Flat$nid"))
          handleType(dict, Some(givenName.getOrElse("")+s"Dict$nid"))
        case TupleDictCType(fs) =>
          val ffs = fs.filter(_._2 != EmptyDictCType)
          if (!ffs.isEmpty) { handleType(RecordCType(ffs), givenName) } else { () }
        case _ => ()
      }
    }
  }

  /** Generate a conditional expression 
    * 
    * @param p condition
    * @param thenp string representing generated code if p is true
    * @param elsep string representing generated code if p is false
    * @return string of the code corresponding to the conditional expression
    */
  private def conditional(p: CExpr, thenp: String, elsep: String): String = p match {
    case Constant(true) => s"${ind(thenp)}"
    case _ => s"if({${generate(p)}}) {${ind(thenp)}} else {${ind(elsep)}}"
  }

  /** Code generator for local scala applications
    *
    * @deprecated this has not been maintained
    * @param e plan that from the BaseANF generator
    * @return string corresponding to the scala application of the input plan
    */
  def generate(e: CExpr): String = e match {
    case Variable(name, _) => name
    case InputRef(name, tp) => 
      handleType(tp, Some("Input_"+name))
      name
    case Comprehension(e1, v, p, e) =>
      val acc = "acc" + Variable.newId()
      val cur = generate(v)
      e.tp match {
        case IntType =>
          s"${generate(e1)}.foldLeft(0)(($acc, $cur) => \n${ind(conditional(p, s"$acc + {${generate(e)}}", s"$acc"))})"
        case DoubleType =>
          s"${generate(e1)}.foldLeft(0.0)(($acc, $cur) => \n${ind(conditional(p, s"$acc + {${generate(e)}}", s"$acc"))})"
        case _ => 
          s"${generate(e1)}.flatMap($cur => { \n${ind(conditional(p, s"${generate(e)}", "Nil"))}})"
      }
    case Record(fs) => {
      val tp = e.tp
      handleType(tp)
      s"${generateType(tp)}(${fs.map(f => generate(f._2)).mkString(", ")}, newId)"
    }
    case Tuple(fs) => s"(${fs.map(f => generate(f)).mkString(",")})"
    case Project(e, field) => e.tp match {
      case _ => s"${generate(e)}.${kvName(field)}"
    }
    case Equals(e1, e2) => s"${generate(e1)} == ${generate(e2)}"
    case Lt(e1, e2) => s"${generate(e1)} < ${generate(e2)}"
    case Gt(e1, e2) => s"${generate(e1)} > ${generate(e2)}"
    case Lte(e1, e2) => s"${generate(e1)} <= ${generate(e2)}"
    case Gte(e1, e2) => s"${generate(e1)} >= ${generate(e2)}"
    case And(e1, e2) => s"${generate(e1)} && ${generate(e2)}"
    case Or(e1, e2) => s"${generate(e1)} || ${generate(e2)}"
    case Not(e1) => s"!(${generate(e1)})"
    case Constant(x) => x match {
      case s:String => "\"" + s + "\""
      case _ => x.toString
    }
    case Sng(e) => s"List(${generate(e)})"
    case WeightedSng(e, q) => s"(1 to ${generate(q)}.asInstanceOf[Int]).map(v => ${generate(e)})"
    case CUnit => "()"
    case EmptySng => "Nil"
    case If(cond, e1, e2) => 
      val zero = e1.tp match { case IntType => 0; case DoubleType => 0.0; case _ => Nil }
      e2 match {
        case Some(a) => s"""
          | if ({${generate(cond)}}) {
          | {${ind(generate(e1))}})
          | } else {
          | {${ind(generate(a))}}
          | }""".stripMargin
        case None => s"""
          | if ({${generate(cond)}})
          | {${ind(generate(e1))}}
          | else $zero """.stripMargin
      }
    case Merge(e1, e2) => s"${generate(e1) ++ generate(e2)}"
    case CDeDup(e1) => s"${generate(e1)}.map(_.toRec).distinct"
    case Bind(x, CNamed(n, e), e2) => s"val $n = ${generate(e)}\nval ${generate(x)} = $n\n${generate(e2)}"
    case LinearCSet(exprs) => 
      s"""(${exprs.map(generate(_)).mkString(",")})"""
    case EmptyCDict => "()"
    case TupleCDict(fs) => generate(Record(fs))
    case Select(x, v, p, e) => p match {
      case Constant(true) => generate(x)
      case _ => s"${generate(x)}.filter(${generate(v)} => ${generate(p)})"
    }
    case Reduce(e1, v, f, p) => 
      s"${generate(e1)}.map{ case ${generateVars(v, e1.tp.asInstanceOf[BagCType].tp)} => { \n${ind(generate(f))} }}"
    case Unnest(e1, v1, f, v2, p, _) => 
      val vars = generateVars(v1, e1.tp.asInstanceOf[BagCType].tp)
      val gv2 = generate(v2)
      s"""|${generate(e1)}.flatMap{ case $vars => 
          |${ind(generate(f))}.flatMap($gv2 => {
          |${ind(s"${conditional(p, s"List(($vars, $gv2))", "Nil")}")}
          |})}""".stripMargin
    case Nest(e1, v1, f, e2, v2, p, g, dk) => 
      val grps = "grps" + Variable.newId()
      val acc = "acc"+Variable.newId()
      val vars = generateVars(v1, e1.tp.asInstanceOf[BagCType].tp)
      val gv2 = generate(v2)
      val np3 = if (v1.size > 1) { 
        val nv = if (v1.last.tp == IntType) { 0 } else { "null" }
        val sv = s"${vars.split(",").dropRight(1).mkString(",")}, $nv)"
        s"case $sv =>  Nil" 
      } else { "" } 
      val nonet = g match {
        case Bind(_, Tuple(fs), _) if fs.size != 1 => s"(${fs.tail.map(e => null).mkString(",")},_)"
        case _ => "(null)"
      }
      val grped = s"{ val $grps = ${generate(e1)}.groupBy{ case $vars => { ${generate(f)} } }"
      val chkg = g match {
        case Bind(_, t @ Tuple(fs), _) => generate(t)
        case _ => "_"
      }
      e2.tp match {
        case IntType => 
          s"""|$grped\n $grps.toList.map($gv2 => ($gv2._1, $gv2._2.foldLeft(0){ 
              | case ($acc, $vars) => {${generate(g)}} match {
              |   case $nonet => $acc
              |   case _ => $acc + {${generate(e2)}}
              | }
              |} ) ) }""".stripMargin
        case DoubleType => 
          s"""|$grped\n $grps.toList.map($gv2 => ($gv2._1, $gv2._2.foldLeft(0.0){ 
              | case ($acc, $vars) => {${generate(g)}} match {
              |   case $nonet => $acc
              |   case _ => $acc + {${generate(e2)}}
              | }
              |} ) ) }""".stripMargin
        case _ => 
          s"""|$grped\n $grps.toList.map($gv2 => ($gv2._1, $gv2._2.flatMap{ 
              |   $np3
              |   case $vars => {${generate(g)}} match {
              |   case $nonet => Nil
              |   case $chkg => List({${generate(e2)}})
              | }
              |} ) ) }""".stripMargin 
      }
    case Lookup(e1, e2, v1, p1, v2, p2, p3) => 
      val hm = "hm" + Variable.newId()
      val vars = generateVars(v1, e1.tp.asInstanceOf[BagCType].tp)
      val gv2 =  generate(v2)
      p3 match {
        case Constant(true) =>
          s"""|{ val $hm = ${generate(e1)}.groupBy{case $vars => { ${generate(p1)} } }
              | ${generate(e2)}.flatMap{$gv2 => $hm.get($gv2._1) match {
              | case Some(a) => a.map(a1 => (a1, $gv2._2))
              | case _ => Nil
              |}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
              |}""".stripMargin
        case _ => 
          s"""|{ val $hm = ${generate(e1)}.groupBy{case $vars => { ${generate(p1)} } }
              | val join1 = ${generate(e2)}.flatMap{$gv2 => $hm.get($gv2._1) match {
              | case Some(a) => $gv2._2
              | case _ => Nil
              | }}
              | val join2 = ${generate(e1)}.groupBy{case $vars => { ${generate(p3)} } }
              | join1.flatMap($gv2 => join2.get({ ${generate(p2)} }) match {
              |   case Some(a) => a.map(a1 => (a1, $gv2))
              |   case _ => Nil
              | })
              |}""".stripMargin
        }
    case OuterLookup(e1, e2, v1, p1, v2, p2, p3) => 
      val hm = "hm" + Variable.newId()
      val vars = generateVars(v1, e1.tp.asInstanceOf[BagCType].tp)
      val gv2 =  generate(v2)
      val np3 = if (v1.size > 1) {
        val sv = s"${vars.split(",").dropRight(1).mkString(",")}, null)"
        s"case $sv =>  List(($sv, null))"
      } else { "" }
      val nonet = p1 match {
        case Bind(_, Tuple(fs), _) => fs.map(e => null).mkString("(", ",", ")")
        case _ => "(null)" 
      }
      s"""|{ val $hm = ${generate(e2)}.map{ case $gv2 => ($gv2._1, $gv2._2)}.toMap
          |${generate(e1)}.flatMap{ 
          |  $np3
          |  case $vars => $hm.get({${generate(p1)}}) match {
          |   case Some(a) => a.flatMap{ case $gv2 => 
          |     if ({${generate(p2)}} == {${generate(p3)} }) { List(($vars, $gv2)) } else { List(($vars, null)) }
          |   }
          |   case _ => List(($vars, null))
          | }
          |}}""".stripMargin
    case Join(e1, e2, v1, p1, v2, p2, proj1, proj2) =>
      val hm = "hm" + Variable.newId()
      val vars = generateVars(v1, e1.tp.asInstanceOf[BagCType].tp)
      (p1, p2) match {
        case (Constant(true), Constant(true)) => 
        s"""|${generate(e2)}.flatMap{ ${generate(v2)} => 
            | ${generate(e1)}.map{ $vars => ($vars, ${generate(v2)}) }
            |}""".stripMargin
        case _ => 
        s"""|{ val $hm = ${generate(e1)}.groupBy{ case $vars => {${generate(p1)}} }
            |${generate(e2)}.flatMap(${generate(v2)} => $hm.get({${generate(p2)}}) match {
            | case Some(a) => a.map(v => (v, ${generate(v2)}))
            | case _ => Nil
            |}) }""".stripMargin
       }
    case OuterJoin(e1, e2, v1, p1, v2, p2, proj1, proj2) => generate(Join(e1, e2, v1, p1, v2, p2, proj1, proj2))
    case OuterUnnest(e1, v1, f, v2, p,_) => //generate(Unnest(e1, v1, f, v2, p))
      val vars = generateVars(v1, e1.tp.asInstanceOf[BagCType].tp)
      val gv2 = generate(v2)
      s"""|${generate(e1)}.flatMap{ case $vars => 
          |${ind(generate(f))}.flatMap($gv2 => {
          |${ind(s"${conditional(p, s"List(($vars, $gv2))", s"List(($vars, null))")}")}
          |})}""".stripMargin
    case Bind(v, e1, e2) =>
      s"val ${generate(v)} = ${generate(e1)} \n${generate(e2)} "
    case _ => sys.error("not supported "+e)
  }

  private def toList(e: String): String = e match {
    case _ if e.startsWith("(") => s"List$e"
    case _ => s"List($e)"
  }

  /** Generates a set of input vars based on a type 
    * ie. (a,b,c) => ((a,b),c)
    *
    * @param e list of variables from operator
    * @param tp type associated to the tupled variables
    * @return string tupled representation of variable list
    */
  private def generateVars(e: List[Variable], tp: Type): String = tp match {
    case TTupleType(seq) if (seq.size == 2 && seq.head == IntType) => s"${generate(e.head)}"
    case TTupleType(seq) if e.size == seq.size => e.map(generate).mkString("(", ", ", ")")
    case TTupleType(seq) if e.size > seq.size => {
      val en = e.dropRight(seq.size - 1)
      val rest = e.takeRight(seq.size - 1).map(generate).mkString(", ")
      s"(${generateVars(en, seq.head)}, $rest)"
    }
    case TTupleType(seq) => sys.error(s"not supported ${e.size} ${seq.size} --> $e:\n ${generateType(tp)}")
    case _ if e.size == 1 => s"${generate(e.head)}"
  }  

}