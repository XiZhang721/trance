package framework.shell

import java.io.File

import framework.examples.tpch.TPCHBase
import framework.generator.spark.AppWriter
import framework.shell.NRCQueries.{ProgramCLI, Query}

import scala.collection.mutable.ListBuffer
import sys.process._
//import scala.sys.process._
import scala.io.{Source, StdIn}
import scala.tools.nsc.interpreter.JavapTool.Input

object App {
  val PATH: String = System.getProperty("user.dir") + "/src/main/scala/framework/shell/"
  val QUERY_PATH: String = System.getProperty("user.dir") + "\\src\\main\\scala\\framework\\examples\\shell\\"
  var editor_command = "notepad.exe" // default editor for Windows
  var isExecuting:Boolean = true
  var print_intermediate:Boolean = false

  val basePath: String = new java.io.File(".").getCanonicalPath
  val sparkExecutorPath: String = new java.io.File(basePath).getParentFile.getAbsolutePath + "\\executor\\spark"
  val workingDirectory = new java.io.File(sparkExecutorPath)
  var sbtPackageCmd:String = ""
  var sparkSubmitCmdHead:String = ""
  val sparkSubmitCmdTail:String = " --master local[*] target/scala-2.12/sparkutils_2.12-0.1.jar"

  def main(args: Array[String]): Unit = {
    Initialize()
    isExecuting = true
    while (isExecuting) {
      CheckInputCommand(GetUserInput())
    }
  }

  /**
    * Initialize the CLI before receiving any inputs
    */
  def Initialize(): Unit = {
    PrintTitleText()
    println(Constants.Messages.HelpDoc)
    CheckOS()

    // Update the sbt path and Spark path
    val isRunningInIntelliJ: Boolean = Option(System.getProperty("idea.active")).isDefined ||
      Option(System.getProperty("idea.paths.selector")).isDefined
    if(isRunningInIntelliJ){
      val (sbtPath, sparkPath) = GetConfigPaths(PATH + "config.txt")
      if(sbtPath != ""){
        sbtPackageCmd = "\"" + sbtPath + "\"" + " package"
      }
      if(sparkPath != ""){
        sparkSubmitCmdHead = "\"" + sparkPath + "\" --class sparkutils.generated."
      }
    }else{
      sbtPackageCmd = "sbt package"
      sparkSubmitCmdHead = "spark-submit --class sparkutils.generated."
    }
    Test()

  }

  /**
    * Get the paths from the config file
    * @param filePath the path of config file
    * @return the sbt path and spark-submit path
    */
  def GetConfigPaths(filePath:String): (String, String) ={
    try{
      val lines = Source.fromFile(filePath).getLines().toList
      val sbtPath = lines.find(_.startsWith("sbt_path")).map(_.split("=").map(_.trim).last).getOrElse(throw new Exception(Constants.Messages.SbtNotFound))
      val sparkPath = lines.find(_.startsWith("spark_path")).map(_.split("=").map(_.trim).last).getOrElse(throw new Exception(Constants.Messages.SparkNotFound))
      (sbtPath, sparkPath)
    }catch {
      case e:Exception =>
        println(e.getMessage)
        ("", "")
    }
  }

  // Test queries
  val query1 =
    s"""
    for o in Order union
      {(date := o.o_orderdate, key := o.o_orderkey)}
      """

  val query2 =
    s"""
    for s in Student union
      {(name := s.s_name, id := s.s_studentnumber)}
      """

  val query3 =
    s"""
    for o in Order union
      {(orderKey := o.o_orderkey, custKey:= o.o_custkey, details :=
      for oo in Order union
        if (oo.o_orderkey = o.o_orderkey && oo.o_custkey = o.o_custkey) then
        {(date := oo.o_orderdate, price := oo.o_totalprice)}
      )}
      """

  val query4 =
    s"""
    for o in Query1 union
      {(date := o.date)}
      """


  /**
    * Add the test below to this function for testing
    */
  def Test(): Unit = {
//    BasicWorkflowTest()
  }


  def BasicWorkflowTest():Unit = {
    NRCQueries.UpdateQuery("Query1", query1)
    NRCQueries.AddTable(PATH + "tables/JSON/order.json", "Order")
    NRCQueries.EvalAndSaveLocal("Query1", PATH + "eval_results/Query1.json")
    NRCQueries.SaveQuery("Query1", PATH + "saved/query/"+"Query1.txt")
    print_intermediate = true
    GenerateStandardApplication("Query1")
    ShowGenerated()
  }

  def ShreddedTest():Unit = {
    NRCQueries.UpdateQuery("Query1", query1)
    GenerateShreddedApplication("Query1")
    ShowGenerated()
  }

  def LoadNestedTableTest():Unit = {
    NRCQueries.AddTable(PATH + "tables/JSON/student.json", "Student")
    NRCQueries.UpdateQuery("Query2", query2)
    NRCQueries.EvalAndSaveLocal("Query2", PATH + "eval_results/Query2.json")
  }

  def EvalAndSaveQueryTest():Unit = {
    NRCQueries.AddTable(PATH + "tables/JSON/order.json", "Order")
    NRCQueries.UpdateQuery("Order1", query1)
    NRCQueries.EvalAndSaveLocal("Order1", PATH + "eval_results/order1_test.json")
    NRCQueries.EvalAndSaveTable("Order1")
    NRCQueries.SaveQuery("Order1",  PATH + "saved/query/order1.txt")
  }

  def LoadQueryTest():Unit = {
    NRCQueries.AddTable(PATH + "tables/JSON/order.json", "Order")
    NRCQueries.LoadQuery(PATH + "saved/query/order1.txt")
    NRCQueries.EvalAndSaveTable("Order1")
  }

  def NestedQueryTest():Unit = {
    NRCQueries.AddTable(PATH + "tables/JSON/order.json", "Order")
    NRCQueries.UpdateQuery("Query3", query3)
    NRCQueries.EvalAndSaveLocal("Query3",PATH + "eval_results/Query3.json")
  }

  def EvalAndSaveProgramTest():Unit = {
    NRCQueries.AddTable(PATH + "tables/JSON/order.json", "Order")
    NRCQueries.UpdateQuery("Query1", query1)
    NRCQueries.UpdateQuery("Temp", query4)
    NRCQueries.RenameQuery("Temp", "Query4")
    NRCQueries.UpdateProgram("Program1", List("Query1","Query4"))
    NRCQueries.PrintProgram("Program1")
    NRCQueries.EvalProgram("Program1")
    GenerateStandardProgramApplication("Program1")
    NRCQueries.SaveProgram("Program1",PATH + "saved/program/Program1.txt")
  }

  def LoadProgramAndGenerationTest():Unit = {
    NRCQueries.LoadProgram(PATH + "saved/program/Program1.txt")
    print_intermediate = true
    GenerateStandardProgramApplication("Program1")
  }

  // Test Finished

  /**
    * Set the external editor based on OS
    */
  def CheckOS(): Unit = {
    val os = System.getProperty("os.name").toLowerCase
    if (os.contains("win")) {
      editor_command = "notepad.exe"
    } else if (os.contains("nix") || os.contains("nux") || os.contains("aix")) {
      editor_command = "nano"
    } else if (os.contains("mac")) {
      editor_command = "vi"
    } else {
      throw new UnsupportedOperationException("Unsupported operating system: " + os)
    }
  }


  /**
    * Read the user input
    * @return the inputted String
    */
  def GetUserInput(): String = {
    print(Constants.PROMPT)

    var line = StdIn.readLine()
    if (line.contains(Constants.Commands.COMMAND)) {
      line
    } else {
      val result: StringBuilder = new StringBuilder
      result.++=(line)
      result.++=(" ")
      result.++=("\n")
      var isFinished = false
      while (!isFinished) {
        line = StdIn.readLine()
        if (line.endsWith(";")) {
          line = line.dropRight(1)
          isFinished = true
        }
        result.++=(line)
        result.++=("\n")
      }
      result.mkString
    }
  }

  def PrintInvalidMessage(): Unit = {
    println(Constants.Messages.InvalidCommand)
  }

  /**
    * Handle the user inputs
    * @param input the received command
    */
  def CheckInputCommand(input: String): Unit = {
//    println(s"Received Command: $input") // This is for debugging
    val input_split = input.toLowerCase().split("\\s+")
    val command = input_split(0)
    if(!command.contains(Constants.Commands.COMMAND)){
      WriteQueryInCommand(input)
    }else if (input_split.length == 1) {
      command match {
        case Constants.Commands.EXIT => isExecuting = false
        case Constants.Commands.HELP => PrintHintText()
        case Constants.Commands.SHOW => NRCQueries.PrintQueryAndProgram()
        case _ => PrintInvalidMessage()
      }
    } else {
      command match {
        case Constants.Commands.QUERY => WriteQueryInEditor(input)
        case Constants.Commands.SHOW => Show(input)
        case Constants.Commands.GENERATE => Generate(input)
        case Constants.Commands.LOAD => Load(input)
        case Constants.Commands.RENAME => RenameQuery(input)
        case Constants.Commands.EVALUATE => Eval(input)
        case Constants.Commands.SAVE => Save(input)
        case Constants.Commands.PROGRAM => MakeProgram(input)
        case Constants.Commands.EXECUTE => Execute(input)
        case _ => PrintInvalidMessage()
      }
    }
  }

  /**
    * Load Query or Program or table
    * @param input the received command
    */
  def Load(input: String):Unit = {
    val input_split = input.split("\\s+")
    if(input_split.length == 4 && input_split(1) == Constants.Commands.OPTION_TABLE){
      val table_name = input_split(2)
      val table_path = input_split(3)
      NRCQueries.AddTable(PATH + "/tables/JSON/" + table_path, table_name)
    }else if(input_split.length == 3) {
      if (input_split(1) == Constants.Commands.OPTION_QUERY) {
        val path = PATH + "saved/query/" + input_split(2)
        NRCQueries.LoadQuery(path)
      } else if (input_split(1) == Constants.Commands.OPTION_PROGRAM) {
        val path = PATH + "saved/program/" + input_split(2)
        NRCQueries.LoadProgram(path)
      } else {
        PrintInvalidMessage()
      }
    }else{
      PrintInvalidMessage()
    }
  }

  /**
    * Save a Query or Program
    * @param input
    */
  def Save(input: String):Unit = {
    val input_split = input.toLowerCase().split("\\s+")
    if(input_split.length == 4){
      val name = input_split(2)
      if(input_split(1) == Constants.Commands.OPTION_QUERY){
        val path = PATH + "saved/query/" + input_split(3) +".txt"
        NRCQueries.SaveQuery(name, path)
      }else if(input_split(1) == Constants.Commands.OPTION_PROGRAM){
        val path = PATH + "saved/program/" + input_split(3) +".txt"
        NRCQueries.SaveProgram(name,path)
      }else{
        PrintInvalidMessage()
      }
    }else{
      PrintInvalidMessage()
    }
  }

  /**
    * Construct the query from input command
    * @param input the received command
    */
  def WriteQueryInCommand(input:String):Unit = {
    try{
      val instruction_split = input.split("\\s+")
      if (instruction_split(1)!= Constants.Commands.ASSIGN){
        println(Constants.Messages.InvalidQueryFormat)
      }else{
        val name:String = instruction_split(0)
        val pattern = """^\s*\w+\s*:=\s*""".r
        val expr:String = pattern.replaceFirstIn(input, "")
        NRCQueries.UpdateQuery(name, expr)
      }
    }catch {
      case _:Exception => println(Constants.Messages.InvalidQueryFormat)
    }

  }

  /**
    * Launch the editor and construct the query from editor contents
    * @param input the received command
    */
  def WriteQueryInEditor(input:String): Unit = {
    try{
      val instruction_split = input.split("\\s+")
      if(instruction_split(1) == Constants.Commands.EDITOR){
        if(instruction_split.length > 2){
          println(Constants.Messages.InvalidQueryFormat)
        }else{
          val query = LaunchEditor()
          val query_split = query.split("\\s+")
          if(query_split(1) != Constants.Commands.ASSIGN){
            println(Constants.Messages.InvalidQueryFormat)
          }else{
            val name = query_split(0)
            val pattern = """^\s*\w+\s*:=\s*""".r
            val expr:String = pattern.replaceFirstIn(query, "")
            NRCQueries.UpdateQuery(name, expr)
          }
        }
      }else{
        PrintInvalidMessage()
      }
    }catch{
      case _:Exception=>println(Constants.Messages.InvalidQueryFormat)
    }

  }

  /**
    * Create an NRC program from several queries
    * @param input the received command
    */
  def MakeProgram(input: String):Unit = {
    try{
      val input_split = input.split("\\s+")
      if(input_split(2) != Constants.Commands.ASSIGN){
        println(Constants.Messages.InvalidProgramFormat)
      }else{
        val program_name = input_split(1)
        val queries = input_split.drop(3).toList
        NRCQueries.UpdateProgram(program_name, queries)
      }
    }catch{
      case _:Exception=>PrintInvalidMessage()
    }

  }

  /**
    * Print contents
    * @param input the received command
    */
  def Show(input:String):Unit = {
    try{
      val input_split = input.split("\\s+")
      if(input_split.length == 3){
        val name = input_split(2)
        val option = input_split(1)
        if(option == Constants.Commands.OPTION_QUERY ){
          NRCQueries.PrintQuery(name)
        }else if(option == Constants.Commands.OPTION_PROGRAM){
          NRCQueries.PrintProgram(name)
        }else{
          PrintInvalidMessage()
        }
      }else if(input_split.length == 2){
        val option = input_split(1)
        if(option == Constants.Commands.OPTION_GENERATED){
          ShowGenerated()
        }else{
          PrintInvalidMessage()
        }
      }else{
        PrintInvalidMessage()
      }
    }catch {
      case _:Exception=>PrintInvalidMessage()
    }
  }

  def RenameQuery(input: String):Unit = {
    try{
      val instruction_split = input.split("\\s+")
      if(instruction_split(2)!= Constants.Commands.TO && instruction_split.length != 4){
        PrintInvalidMessage()
      }else{
        val name_old = instruction_split(1)
        val name_new = instruction_split(3)
        NRCQueries.RenameQuery(name_old, name_new)
      }
    }catch {
      case _:Exception=>PrintInvalidMessage()
    }

  }


  /**
    * The Spark generation
    * @param input the received command
    */
  def Generate(input:String):Unit = {
    try{
      val input_split = input.split("\\s+")
      if(input_split.length == 3 && input_split(1)== Constants.Commands.INTERMEDIATE){
        if(input_split(2) == Constants.Commands.ON){
          print_intermediate = true
          println(s"You have turned ${Constants.Commands.ON} the intermediate states printing for Spark generation.")
        }else if(input_split(2) == Constants.Commands.OFF){
          print_intermediate = false
          println(s"You have turned ${Constants.Commands.OFF} the intermediate states printing for Spark generation.")
        }else{
          PrintInvalidMessage()
        }
      }else if(input_split.length == 3){
        val query = input_split.drop(2)
        if(input_split(1) == Constants.Commands.STANDARD){
          GenerateStandardApplication(query(0))
        }else if(input_split(1) == Constants.Commands.SHREDDED){
          GenerateShreddedApplication(query(0))
        }else{
          PrintInvalidMessage()
        }
      }else if(input_split.length == 4){
        val program = input_split.drop(3)
        if(input_split(2) == Constants.Commands.OPTION_PROGRAM){
          if(input_split(1) == Constants.Commands.STANDARD){
            GenerateStandardProgramApplication(program(0))
          }else if(input_split(1) == Constants.Commands.SHREDDED){
            GenerateShreddedApplication(program(0))
          }else{
            PrintInvalidMessage()
          }
        }else{
          PrintInvalidMessage()
        }
      }else{
        PrintInvalidMessage()
      }
    }catch {
      case _:Exception=>PrintInvalidMessage()
    }

  }

  /**
    * Spark generation for NRC query using standard pipeline
    * @param name the query name
    */
  def GenerateStandardApplication(name:String): Unit = {
    val label = s"ExperimentShell, $name, standard"
    try{
      val query:Query = NRCQueries.GetQuery(name)
      val parsed_query = NRCQueries.Parse(query.query_expression)
      object ShellQuery extends TPCHBase {
        val tbls: Set[String] =Set("Customer", "Order","Lineitem","Part","Region","PartSupp","Nation","Supplier")
        val name: String = query.query_name
        val program: ShellQuery.Program = Program(Assignment(name,parsed_query.asInstanceOf[Expr]))
      }
      if(print_intermediate){
        AppWriter.runDatasetWithIntermediatePrinting(ShellQuery, label, optLevel = 1)
      }else{
        AppWriter.runDataset(ShellQuery, label, optLevel = 1)
      }

    }catch {
      case e: Exception => {
        println("Generation for " + label + " using the standard pipeline failed")
        e.printStackTrace()
      }
    }
  }

  /**
    * Spark generation for NRC query using shredded pipeline
    * @param name the query name
    */
  def GenerateShreddedApplication(name:String): Unit = {
    val label = s"ExperimentShell, $name, shredded"
    try{
      val query:Query = NRCQueries.GetQuery(name)
      val parsed_query = NRCQueries.Parse(query.query_expression)
      object ShellQuery extends TPCHBase {
        val tbls: Set[String] =Set("Customer", "Order","Lineitem","Part","Region","PartSupp","Nation","Supplier")
        val name: String = query.query_name
        val program: ShellQuery.Program = Program(Assignment(name,parsed_query.asInstanceOf[Expr]))
      }
      if(print_intermediate){
        AppWriter.runDatasetShredWithIntermediatePrinting(ShellQuery, label, optLevel = 1)
      }
      else{
        AppWriter.runDatasetShred(ShellQuery, label, optLevel = 1)
      }
    }catch {
      case e: Exception => {
        println("Generation for " + label + " using the shredded pipeline failed")
        e.printStackTrace()
      }
    }
  }

  /**
    * Spark generation for NRC program using standard pipeline
    * @param program_name the program name
    */
  def GenerateStandardProgramApplication(program_name:String): Unit = {
    val label = s"ExperimentShell, $program_name, standard"
    try{
      object ShellQuery extends TPCHBase {
        val tbls: Set[String] =Set("Customer", "Order","Lineitem","Part","Region","PartSupp","Nation","Supplier")
        var exprs: List[(String, NRCQueries.Expr)] = NRCQueries.ParseProgram(program_name)
        val assignments: ListBuffer[Assignment] = ListBuffer.empty[Assignment]
        val name: String = program_name
        for ((q_name, q_expr)<-exprs){
          assignments += Assignment(q_name, q_expr.asInstanceOf[Expr])
        }

        val program: ShellQuery.Program = Program(assignments.toList: _*)
      }
      if(print_intermediate){
        AppWriter.runDatasetWithIntermediatePrinting(ShellQuery, label, optLevel = 1)
      }else{
        AppWriter.runDataset(ShellQuery, label, optLevel = 1)
      }

    }catch {
      case e: Exception => {
        println("Generation for " + label + " using the standard pipeline failed")
        e.printStackTrace()
      }
    }
  }

  /**
    * Spark generation for NRC program using shredded pipeline
    * @param program_name the program name
    */
  def GenerateShreddedProgramApplication(program_name:String): Unit = {
    val label = s"ExperimentShell, $program_name, shredded"
    try{
      object ShellQuery extends TPCHBase {
        val tbls: Set[String] =Set("Customer", "Order","Lineitem","Part","Region","PartSupp","Nation","Supplier")
        var exprs: List[(String, NRCQueries.Expr)] = NRCQueries.ParseProgram(program_name)
        val assignments: ListBuffer[Assignment] = ListBuffer.empty[Assignment]
        val name: String = program_name
        for ((q_name, q_expr)<-exprs){
          assignments += Assignment(q_name, q_expr.asInstanceOf[Expr])
        }

        val program: ShellQuery.Program = Program(assignments.toList: _*)
      }
      if(print_intermediate){
        AppWriter.runDatasetShredWithIntermediatePrinting(ShellQuery, label, optLevel = 1)
      }
      else{
        AppWriter.runDatasetShred(ShellQuery, label, optLevel = 1)
      }
    }catch {
      case e: Exception => {
        println("Generation for " + label + " using the shredded pipeline failed")
        e.printStackTrace()
      }
    }
  }

  /**
    * launch the external editor for receiving user inputs
    * @return the contents written iin the editor
    */
  def LaunchEditor():String = {
    println(Constants.LineBreak)
    println(Constants.Messages.WriteQuery)

    val tempFile = File.createTempFile("TempQueryEditor", ".txt")
    try{
      val process = Process(editor_command, Seq(tempFile.getAbsolutePath)).run()
      process.exitValue()

      val source = Source.fromFile(tempFile)
      val writtenQuery = try source.mkString finally source.close()

      writtenQuery
    }finally{
      tempFile.delete()
    }
  }

  /**
    * Check whether eval query or program
    * @param input the received command
    */
  def Eval(input:String):Unit = {
    if(input.split("\\s+")(1) == Constants.Commands.OPTION_PROGRAM){
      EvalProgram(input)
    }else{
      EvalQuery(input)
    }
  }

  /**
    * Eval NRC query
    * @param input the received command
    */
  def EvalQuery(input:String):Unit = {
    val instruction_split = input.split("\\s+")
    if(instruction_split.length == 2){
      val name = instruction_split(1)
      NRCQueries.EvalAndSaveTable(name)
    }else if(instruction_split.length == 4 && instruction_split(2) == Constants.Commands.TO){
      val name = instruction_split(1)
      val path = instruction_split(3)
      NRCQueries.EvalAndSaveLocal(name, PATH + "eval_results/" + path)
    }else{
      PrintInvalidMessage()
    }
  }

  /**
    * Eval NRC program
    * @param input the received command
    */
  def EvalProgram(input:String):Unit = {
    val instruction_split = input.split("\\s+")
    if(instruction_split.length == 3){
      val name = instruction_split(2)
      NRCQueries.EvalProgram(name)
    }else{
      PrintInvalidMessage()
    }
  }

  /**
    * Package the executor
    */
  def Package():Unit = {
    if(sbtPackageCmd == ""){
    println(Constants.Messages.SbtNotProvided)
    return
    }else{
      println(Constants.Messages.PackageOngoing)
      val commandArray = Array("cmd", "/c", sbtPackageCmd)
      val process = Process(commandArray, workingDirectory)
      val result = process.!

      // Check result
      if (result == 0) {
        println(Constants.Messages.PackageSuccess)
      } else {
        println(Constants.Messages.PackageFail)
      }
    }
  }

  /**
    * Show the name of Spark files
    */
  def ShowGenerated():Unit = {
    val directoryPath = sparkExecutorPath + "\\src\\main\\scala\\sparkutils\\generated"
    val dir = new File(directoryPath)
    if (dir.exists && dir.isDirectory) {
      val scalaFiles = dir.listFiles.filter(_.isFile).filter(_.getName.endsWith(".scala"))
      scalaFiles.foreach(file => println(file.getName.dropRight(6)))
    } else {
      println(Constants.Messages.InvalidDirectory)
    }
  }

  /**
    * Execute the Spark applications
    * @param input the name of Spark file
    */
  def Execute(input:String):Unit = {
    if(sparkSubmitCmdHead == ""){
      println(Constants.Messages.SparkNotProvided)
      return
    }else{
      try{
        Package()
        val input_split = input.split("\\s+")
        if(input_split.length == 2){
          val name = input_split(1)
          val sparkSubmitCmd = sparkSubmitCmdHead + name + sparkSubmitCmdTail
          val commandArray = Array("cmd", "/c", sparkSubmitCmd)
          println(Constants.Messages.ExecutionOngoing)
          val process = Process(commandArray, workingDirectory)
          val result = process.!

          // Check result
          if (result == 0) {
            println(Constants.Messages.ExecutionSuccess)
          } else {
            println(Constants.Messages.ExecutionFail)
          }
        }else if(input_split.length == 3 && input_split(1)==Constants.Commands.OPTION_SAVE){
          val name = input_split(2)
          val path = PATH + "execution/"+name + ".txt"
          val sparkSubmitCmd = sparkSubmitCmdHead + name + sparkSubmitCmdTail +" > " + path
          val commandArray = Array("cmd", "/c", sparkSubmitCmd)
          println(Constants.Messages.ExecutionOngoing)
          val process = Process(commandArray, workingDirectory)
          val result = process.!
          if (result == 0) {
            println(Constants.Messages.ExecutionSuccess)
            println("Execution result saved to: " + path)
          } else {
            println(Constants.Messages.ExecutionFail)
          }
        }else{
          PrintInvalidMessage()
        }
      }catch{
        case e:Exception =>{
          println(Constants.Messages.FailSparkExecution)
          e.printStackTrace()
        }
      }
    }
  }

  /**
    * Prints the TraNCE title texts
    */
  def PrintTitleText():Unit = {
    val title_source = Source.fromFile(PATH + "title.txt")
    for (line <- title_source.getLines) {
      println(line)
    }
    title_source.close()
  }

  /**
    * Prints out the Hint texts which are the help documentations
    */
  def PrintHintText():Unit = {
    val hint_source = Source.fromFile(PATH + "hint.txt")
    for (line <- hint_source.getLines) {
      println(line)
    }
    hint_source.close()
  }

}
