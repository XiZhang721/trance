package framework.shell

object Constants {

  val PROMPT: String = "$Trance> "
  val LineBreak:String = "********************************************"
  object Commands {
    // Commands
    val COMMAND:String = "\\"
    val EXIT:String = "\\exit"
    val ADD:String = "\\add"
    val FROM:String = "\\from"
    val HELP:String = "\\help"
    val WRITE:String = "\\write"
    val QUERY:String = "\\query"
    val EDITOR:String = "-editor"
    val PROGRAM:String = "\\program"
    val SHOW:String = "\\show"
    val FORMAT:String = "-formatted"
    val RENAME:String = "\\rename"
    val TO:String = "to"
    val SAVE:String = "\\save"
    val OPTION_QUERY:String = "-query"
    val OPTION_PROGRAM:String = "-program"
    val OPTION_TABLE:String = "-table"
    val OPTION_GENERATED:String = "-generated"
    val OPTION_SAVE:String = "-save"
    val CLEAR:String = "\\clear"
    val EVALUATE:String = "\\evaluate"
    val GENERATE:String = "\\generate"
    val STANDARD:String = "-standard"
    val SHREDDED:String = "-shredded"
    val INTERMEDIATE:String = "-intermediate"
    val LOAD:String = "\\load"
    val ASSIGN:String = ":="
    val ON:String = "on"
    val OFF:String = "off"
    val EXECUTE:String = "\\execute"
  }

  object File {
    // File Generation
    val IDENT:String = "  "
    val PACKAGE:String = "package framework.examples.shell"
    val IMPORT_TPCH:String = "import framework.examples.tpch.TPCHBase"
    val IMPORT_TRANSLATOR = "import framework.shell"
    val OBJECT = "object NRCQuery extends TPCHBase {"
    val TBLS:String = "val tbls: Set[String] =Set(\"Customer\", \"Order\",\"Lineitem\",\"Part\",\"Region\",\"PartSupp\",\"Nation\",\"Supplier\")"
    val PARSE_QUERY = "val parsed_query = shell.NRCQuery.Parse(query)"
    val PROGRAM:String = "val program = Program(Assignment(name,parsed_query.asInstanceOf[Expr]))"
  }

  object SAVE {
    val QUERY:String = "\\QUERY"
    val PROGRAM:String = "\\PROGRAM"
  }

  object Messages {
    val HelpDoc:String = "Please use \\help to see all available commands."
    val InvalidCommand:String = "Invalid command, retry please."
    val SaveBeforeExit:String = "You have not saved your query, do you wish to save it before exiting (yes/no)?"
    val WriteQuery:String = "A new editor window is opened for you to write your query, save and close after finish editing."
    val WriteQueryName:String = "Give a name for your query:"
    val NoValidQuery:String = "You do not have a valid query, write your query first."
    val ClearQuery:String = "Your query has been cleared, workspace is now clean."
    val SaveFailed:String = "Your query is unable to save, it will be cleared."
    val InvalidQueryFormat:String = "Invalid format for writing query"
    val InvalidProgramFormat:String = "Invalid format for creating program"
    val FailSparkExecution:String = "Spark program execution failed."
    val SbtNotFound:String = "sbt_path not found in config.txt file"
    val SparkNotFound:String = "spark_path not found in config.txt file"
    val SbtNotProvided:String = "You have not provided the path for sbt."
    val SparkNotProvided:String = "You have not provided the path for Spark."
    val InvalidDirectory:String = "Directory does not exist or is not a directory"
    val PackageOngoing:String = "Packaging ongoing, please wait..."
    val ExecutionOngoing:String = "Execution ongoing, please wait..."
    val PackageSuccess:String = "Package created successfully."
    val PackageFail:String = "Error occurred during packaging."
    val ExecutionSuccess:String = "Spark application executed successfully."
    val ExecutionFail:String = "Error occurred during Spark execution."
  }
}
