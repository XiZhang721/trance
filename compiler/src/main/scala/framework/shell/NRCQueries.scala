package framework.shell

import java.io._

import framework.common._
import framework.examples.tpch.TPCHSchema
import framework.nrc.Parser
import framework.{nrc, runtime}
import framework.runtime.RuntimeContext
import play.api.libs.json._

import scala.collection.mutable.ListBuffer
import scala.io.Source

object NRCQueries extends nrc.MaterializeNRC with nrc.Printer with runtime.Evaluator {
  case class Query(query_name:String, query_expression:String)
  case class ProgramCLI(program_name:String, queries:List[Query])

  var tbls: Map[String, BagType] = Map(
    "Customer" -> TPCHSchema.customertype,
    "Order" -> TPCHSchema.orderstype,
    "Lineitem" -> TPCHSchema.lineittype,
    "Part" -> TPCHSchema.parttype,
    "Region" -> TPCHSchema.regiontype,
    "PartSupp" -> TPCHSchema.partsupptype,
    "Nation" -> TPCHSchema.nationtype,
    "Supplier" -> TPCHSchema.suppliertype
  )

  val rtx = new RuntimeContext()

  private val queries: scala.collection.mutable.Map[String, Query] = scala.collection.mutable.Map()
  private val programs: scala.collection.mutable.Map[String, ProgramCLI] = scala.collection.mutable.Map()

  /**
    * Parse an NRC query
    * @param query the query string
    * @param parser the parser
    * @return the parsed NRC expression
    */
  def Parse(query: String, parser:Parser = nrc.Parser(tbls)): Expr={
    try{
      val query_parsed = parser.parse(query, parser.term).get.asInstanceOf[Expr]
      query_parsed
    }catch{
      case e:Exception =>
        e.printStackTrace()
        throw new IllegalArgumentException(s"The query cannot be parsed: \n $query")
    }
  }

  /**
    * Parse an NRC program
    * @param name the program name
    * @return the program expression
    */
  def ParseProgram(name:String):List[(String, Expr)] = {
    try{
      var tbls_temp = tbls
      val program:ProgramCLI = GetProgram(name)
      var assignments: ListBuffer[(String, Expr)] = ListBuffer.empty[(String, Expr)]
      for (query<-program.queries){
        val q_name = query.query_name
        val expr = query.query_expression
        val parsed_query = Parse(expr, nrc.Parser(tbls_temp))
        assignments += ((q_name, parsed_query))
        tbls_temp += (q_name -> parsed_query.asInstanceOf[BagExpr].tp)
      }
      assignments.toList
    }catch {
      case e:Exception =>
        e.printStackTrace()
        throw new IllegalArgumentException(s"The program cannot be parsed: \n $name")
    }
  }

  def UpdateQuery(name:String, expression: String):Unit = {
    val query = Query(name, expression)
    queries(name) = query
  }

  def UpdateProgram(p_name:String, q_names: List[String]):Unit = {
    try{
      val p_queries = scala.collection.mutable.Buffer[Query]()
      for(q_name <- q_names){
        val query:Query = GetQuery(q_name)
        p_queries += query
      }
      programs(p_name) = ProgramCLI(p_name, p_queries.toList)
    }catch{
      case e:Exception=>
        println("Cannot make program because:" + e.getMessage)

    }

  }

  def GetQuery(name:String):Query = {
    val result = queries.get(name) match {
      case Some(value) => value
      case None => throw new IllegalArgumentException(s"Query $name not exist")
    }
    result
  }

  def GetProgram(name:String):ProgramCLI = {
    val result = programs.get(name) match{
      case Some(value) => value
      case None => throw new IllegalArgumentException(s"Program $name not exist")
    }
    result
  }

  def PrintQuery(name:String):Unit = {
    try{
      val query = GetQuery(name)
      println(s"Query $name:")
      println(query.query_expression)
    }catch{
      case _:Exception =>
        println(s"Query $name is not valid for printing.")
    }
  }

  def PrintProgram(name:String):Unit = {
    try{
      val program = GetProgram(name)
      println(s"Program $name:")
      for (query <- program.queries){
        println("----------")
        println("Query " + query.query_name +":")
        println(query.query_expression)
      }
    }catch{
    case _:Exception =>
      println(s"Program $name is not valid for printing.")
    }
  }

  def PrintQueryAndProgram():Unit = {
    if(queries.isEmpty){
      println("You have not written any query yet.")
    }else{
      println("Queries:")
      for ((key, _) <- queries) {
        println(" " + key)
      }
      if(programs.nonEmpty){
        println("Programs:")
        for ((key,value)<-programs){
          var message:String = " " + key + "("
          var isFirst:Boolean = true
          for(query <- value.queries){
            if(!isFirst){
              message += ", "
            }
            message += query.query_name
            isFirst = false
          }
          message += ")"
          println(message)

        }
      }
    }
  }

  def RenameQuery(oldName:String, newName:String):Unit = {
    try{
      val query:Query = GetQuery(oldName)
      val new_query = Query(newName,query.query_expression)
      queries.remove(oldName)
      queries(newName) = new_query
    }catch {
      case _:Exception =>
        println(s"Failed to rename the query $oldName.")
    }
  }

  private def determineType(value: Any): TupleAttributeType = value match {
    case _: Boolean => BoolType
    case _: Int => IntType
    case _: Long => LongType
    case _: Double => DoubleType
    case _: String => StringType
    case v: List[Map[String, Any]] =>BagType(TupleType(v.headOption.getOrElse(Map.empty).map { case (key, value) => key -> determineType(value)}))
    case v: Map[String, Any] =>BagType(TupleType(v.map { case (key, value) => key -> determineType(value)}))
    case _ => throw new IllegalArgumentException("Unsupported type")
  }

  def convertJsValueToAny(jsValue: JsValue): Any = jsValue match {
    case JsNull => null
    case JsString(value) => value
    case JsNumber(value) if value.isValidInt => value.toInt
    case JsNumber(value) if value.isValidLong => value.toLong
    case JsNumber(value) if value.isBinaryDouble => value.toDouble
    case JsNumber(value) if value.isDecimalDouble => value.toDouble
    case JsBoolean(value) => value
    case JsArray(values) => values.map(convertJsValueToAny)
    case JsObject(fields) => fields.map { case (key, value) => key -> convertJsValueToAny(value) }
    case _ => jsValue.toString // Fallback: Convert to string if not recognized
  }

  def convertAnyToJsValue(value: Any): JsValue = value match {
    case v: String => JsString(v)
    case v: Int => JsNumber(v)
    case v: Double => JsNumber(v)
    case v: Long => JsNumber(v)
    case v: Boolean => JsBoolean(v)
    case v: List[Map[String, Any]] =>JsArray(v.map { map =>
      JsObject(map.mapValues(convertAnyToJsValue))
    })
    case _ => JsNull
  }

  def GetBagType(relationRValue:List[Map[String, Any]]): BagType = {
    val firstElement = relationRValue.headOption.getOrElse(Map.empty)
    val fieldsMap: Map[String, TupleAttributeType] = firstElement.map { case (key, value) => key -> determineType(value) }
    val itemTp = TupleType(fieldsMap)
    BagType(itemTp)
  }

  def AddTable(path:String, name:String): Unit = {
    try{
      val source = Source.fromFile(path)
      val jsonString:String = try{
        source.getLines().mkString("\n")
      }catch {
        case _: Exception =>
          //e.printStackTrace()
          ""
      }
      if(jsonString == "") return
      try{
        val json: JsValue = Json.parse(jsonString)
        val relationRValue = json.as[List[Map[String, JsValue]]].map { map =>
          map.mapValues(convertJsValueToAny)
        }
        val bagTp = GetBagType(relationRValue)
        val relationR = BagVarRef(name, bagTp)
        this.rtx.add(relationR.varDef, relationRValue)
        this.tbls += (name -> bagTp)
        //    println(relationRValue)
        //    println(relationR)
        println(s"Table $name loaded from $path")
      }catch {
        case _:Exception =>
          println(s"Table $name failed to load")
      }
    }catch {
      case _:Exception =>
        println(s"Table $name failed to load")
    }

  }

  def GetEvalResult(name:String): List[Map[String, Any]] = {
    val query = GetQuery(name)
    val parsed_query = Parse(query.query_expression, nrc.Parser(tbls))
    eval(parsed_query, rtx).asInstanceOf[List[Map[String, Any]]]
  }

  def AddToEval(q_name:String, eval_result:List[Map[String, Any]]): Unit ={
    val parsed_query = Parse(GetQuery(q_name).query_expression, nrc.Parser(tbls))
    tbls += (q_name -> parsed_query.asInstanceOf[BagExpr].tp)
    val bagTp = GetBagType(eval_result)
    val relationR = BagVarRef(q_name, bagTp)
    this.rtx.add(relationR.varDef, eval_result)
  }

  /**
    * Evaluate a query and save the results to local as JSON file
    * @param name the query name
    * @param path the file path of saving
    */
  def EvalAndSaveLocal(name:String, path:String):Unit = {
    try{
      val eval_result = GetEvalResult(name)
      println(s"Evaluation Result For Query $name: ")
      println(eval_result)
      try{
        AddToEval(name, eval_result)
        val resultSeq: Seq[JsValue] = eval_result.map { map =>
          JsObject(map.map {
            case (key, value) =>
              key -> convertAnyToJsValue(value)
          })
        }
        val result_json = Json.arr(resultSeq)
        val outputFile = new File(path)
        val printWriter = new PrintWriter(outputFile)
        try{
          printWriter.write(Json.prettyPrint(result_json).drop(1).dropRight(1))
          println(s"Evaluation result saved to $path")
        }catch {
          case _:Exception =>
            println(s"Query $name failed to save evaluation result.")
        }finally {
          printWriter.close()
        }
      }catch {
        case _:Exception =>
          println(s"Query $name failed to save evaluation result.")
      }
    }catch{
      case _:Exception =>
        println(s"Query $name failed to evaluate.")
    }
  }

  /**
    * Evaluate a query without saving the results to local as JSON file
    * @param name_query the query name
    */
  def EvalAndSaveTable(name_query:String):Unit = {
    try{
      val eval_result = GetEvalResult(name_query)
      println(s"Evaluation Result For Query $name_query: ")
      println(eval_result)
      AddToEval(name_query, eval_result)
    }catch{
      case _:Exception =>
        println(s"Query $name_query failed to evaluate.")
    }
  }

  def EvalProgram(name_program:String):Unit = {
    try{
      val program:ProgramCLI = GetProgram(name_program)
      val temp_rtx = rtx
      println(s"Evaluation Result for Program $name_program: ")
      for (query <- program.queries){
        val q_name = query.query_name
        val parsed_query = Parse(GetQuery(q_name).query_expression, nrc.Parser(tbls))
        val eval_result = eval(parsed_query, temp_rtx).asInstanceOf[List[Map[String, Any]]]
        println(s"Evaluation Result For Query $q_name: ")
        println(eval_result)
        tbls += (q_name -> parsed_query.asInstanceOf[BagExpr].tp)
        val BagTp = GetBagType(eval_result)
        val relationR = BagVarRef(q_name, BagTp)
        temp_rtx.add(relationR.varDef, eval_result)
      }
    }catch {
      case _:Exception =>{
        println(s"Program $name_program failed to evaluate.")
      }
    }
  }

  def SaveQuery(name:String, path:String):Unit = {
    try{
      val query = GetQuery(name)
      val expr = query.query_expression
      val fileWriter = new FileWriter(path)
      try{
        fileWriter.write(Constants.SAVE.QUERY + " " + name)
        fileWriter.write(expr)
        println(s"Query $name saved to $path")
      }catch {
        case _:Exception =>
          println(s"Query $name failed to save.")
      }finally {
        fileWriter.close()
      }
    }catch {
      case _:Exception =>
        println(s"Query $name failed to save.")
    }
  }

  def LoadQuery(path:String):Unit = {
    try{
      val source = Source.fromFile(path)
      val lines = source.getLines()
      var name:String = ""
      var expr:String = ""
      for (line <- lines){
        if(line.startsWith(Constants.SAVE.QUERY)){
          name = line.substring(7).trim
        }else{
          expr += line
        }
      }
      UpdateQuery(name, expr)
      println(s"Query $name load from $path")
      source.close()
    }catch {
      case _:Exception => println(s"Failed to load query from $path")
    }
  }

  def SaveProgram(name:String, path:String): Unit ={
    try{
      val fileWriter = new FileWriter(path)
      val program = GetProgram(name)
      try{
        fileWriter.write(Constants.SAVE.PROGRAM + " " + name)
        for (query<-program.queries){
          val q_name = query.query_name
          val expr = query.query_expression
          fileWriter.write("\n" + Constants.SAVE.QUERY + " " + q_name)
          fileWriter.write(expr)
        }
        println(s"Program $name saved to $path")
      }catch{
        case _:Exception => println(s"Program $name failed to save.")
      }finally{
          fileWriter.close()
      }
    }catch{
      case _:Exception => println(s"Program $name failed to save.")
    }
  }

  def LoadProgram(path:String):Unit = {
    try{
      val source = Source.fromFile(path)
      val p_queries = scala.collection.mutable.Buffer[Query]()
      val lines = source.getLines()
      var p_name = ""
      var q_name:String = ""
      var expr:String = ""
      for (line <- lines){
        if(!line.isEmpty || !line.isBlank) {
          if(line.startsWith(Constants.SAVE.PROGRAM)){
            p_name = line.substring(9).trim
            q_name = ""
            expr = ""
          }else if(line.startsWith(Constants.SAVE.QUERY)){
            if(q_name != "" || expr != ""){
              UpdateQuery(q_name, expr)
              p_queries += Query(q_name,expr)
            }
            q_name = line.substring(7).trim
            expr = ""
          }else{
            expr += line
          }
        }
      }
      UpdateQuery(q_name, expr)
      p_queries += Query(q_name,expr)
      println(p_name)
      println(p_queries.toList)
      programs(p_name) = ProgramCLI(p_name, p_queries.toList)
      println(s"Program $p_name load from $path")
      source.close()
    }catch{
      case _:Exception => println(s"Failed to load program from $path")
    }
  }

}
