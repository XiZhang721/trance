package framework.shell.tables
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import play.api.libs.json._
import java.io.{File, PrintWriter}

object TableConvert {
  val PATH: String = System.getProperty("user.dir") + "/src/main/scala/framework/shell/tables/"

  trait JsonSerializable {
    def toJson: JsValue
  }

  implicit val serializableWrites: Writes[JsonSerializable] = new Writes[JsonSerializable] {
    def writes(obj: JsonSerializable): JsValue = obj.toJson
  }

  case class Customer(c_custkey: Int, c_name: String, c_address: String, c_nationkey: Int, c_phone: String, c_acctbal: Double, c_mktsegment: String, c_comment: String) extends JsonSerializable{
    override def toJson: JsValue = Json.toJson(this)(Json.writes[Customer])
  }
  object Customer{
    implicit val customerFormat: Format[Customer] = Json.format[Customer]
  }

  case class Lineitem(l_orderkey: Int, l_partkey: Int, l_suppkey: Int, l_linenumber: Int, l_quantity: Double, l_extendedprice: Double, l_discount: Double, l_tax: Double, l_returnflag: String, l_linestatus: String, l_shipdate: String, l_commitdate: String, l_receiptdate: String, l_shipinstruct: String, l_shipmode: String, l_comment: String) extends JsonSerializable {
    override def toJson: JsValue = Json.toJson(this)(Json.writes[Lineitem])
  }
  object Lineitem{
    implicit val lineitemFormat:Format[Lineitem] = Json.format[Lineitem]
  }

  case class Nation(n_nationkey: Int, n_name: String, n_regionkey: Int, n_comment: String) extends JsonSerializable {
    override def toJson: JsValue = Json.toJson(this)(Json.writes[Nation])
  }
  object Nation{
    implicit val NationFormat:Format[Nation] = Json.format[Nation]
  }

  case class Order(o_orderkey: Int, o_custkey: Int, o_orderstatus: String, o_totalprice: Double, o_orderdate: String, o_orderpriority: String, o_clerk: String, o_shippriority: Int, o_comment: String) extends JsonSerializable{
    override def toJson: JsValue = Json.toJson(this)(Json.writes[Order])
  }
  object Order{
    implicit val OrderFormat:Format[Order] = Json.format[Order]
  }

  case class Part(p_partkey: Int, p_name: String, p_mfgr: String, p_brand: String, p_type: String, p_size: Int, p_container: String, p_retailprice: Double, p_comment: String) extends JsonSerializable{
    override def toJson: JsValue = Json.toJson(this)(Json.writes[Part])
  }
  object Part{
    implicit val PartFormat:Format[Part] = Json.format[Part]
  }

  case class PartSupp(ps_partkey: Int, ps_suppkey: Int, ps_availqty: Int, ps_supplycost: Double, ps_comment: String) extends JsonSerializable{
    override def toJson: JsValue = Json.toJson(this)(Json.writes[PartSupp])
  }
  object PartSupp{
    implicit val PartSuppFormat:Format[PartSupp] = Json.format[PartSupp]
  }

  case class Region(r_regionkey: Int, r_name: String, r_comment: String) extends JsonSerializable{
    override def toJson: JsValue = Json.toJson(this)(Json.writes[Region])
  }
  object Region{
    implicit val RegionFormat:Format[Region] = Json.format[Region]
  }

  case class Supplier(s_suppkey: Int, s_name: String, s_address: String, s_nationkey: Int, s_phone: String, s_acctbal: Double, s_comment: String) extends JsonSerializable{
    override def toJson: JsValue = Json.toJson(this)(Json.writes[Supplier])
  }
  object Supplier{
    implicit val SupplierFormat:Format[Supplier] = Json.format[Supplier]
  }

  def ParseCustomer(filepath_in:String = "customer.tbl"):ArrayBuffer[Customer] = {
    val path_in:String = PATH + "TBL/" + filepath_in
    var result:ArrayBuffer[Customer] = ArrayBuffer[Customer]()
    val source = Source.fromFile(path_in)
    try{
      for (line <- source.getLines()) {
        val columns = line.split("\\|")
        try {
          val customer: Customer = Customer(
            c_custkey = columns(0).toInt,
            c_name = columns(1),
            c_address = columns(2),
            c_nationkey = columns(3).toInt,
            c_phone = columns(4),
            c_acctbal = columns(5).toDouble,
            c_mktsegment = columns(6),
            c_comment = columns(7)
          )
          result += customer
        } catch {
          case e: Exception =>
            print(e.getStackTrace)
        }
      }
    } finally {
      source.close()
    }
    result
  }

  def ParseLineItem(filepath_in:String = "lineitem.tbl"):ArrayBuffer[Lineitem] = {
    val path_in:String = PATH + "TBL/" + filepath_in
    var result:ArrayBuffer[Lineitem] = ArrayBuffer[Lineitem]()
    val source = Source.fromFile(path_in)
    try{
      for (line <- source.getLines()){
        val columns = line.split("\\|")
        try {
          val lineitem = Lineitem(
            l_orderkey = columns(0).toInt,
            l_partkey = columns(1).toInt,
            l_suppkey = columns(2).toInt,
            l_linenumber = columns(3).toInt,
            l_quantity = columns(4).toDouble,
            l_extendedprice = columns(5).toDouble,
            l_discount = columns(6).toDouble,
            l_tax = columns(7).toDouble,
            l_returnflag = columns(8),
            l_linestatus = columns(9),
            l_shipdate = columns(10),
            l_commitdate = columns(11),
            l_receiptdate = columns(12),
            l_shipinstruct = columns(13),
            l_shipmode = columns(14),
            l_comment = columns(15),
          )
          result += lineitem
        }catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
    } finally {
      source.close()
    }
    result
  }

  def ParseNation(filepath_in:String = "nation.tbl"):ArrayBuffer[Nation] = {
    val path_in:String = PATH + "TBL/" + filepath_in
    var result:ArrayBuffer[Nation] = ArrayBuffer[Nation]()
    val source = Source.fromFile(path_in)
    try {
      for (line <- source.getLines()) {
        val columns = line.split("\\|")
        try {
          val nation = Nation(
            n_nationkey = columns(0).toInt,
            n_name = columns(1),
            n_regionkey = columns(2).toInt,
            n_comment = columns(3)
          )
          result += nation
        }catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
    } finally {
      source.close()
    }
    result
  }

  def ParseOrder(filepath_in:String = "order.tbl"):ArrayBuffer[Order] = {
    val path_in:String = PATH + "TBL/" + filepath_in
    var result:ArrayBuffer[Order] = ArrayBuffer[Order]()
    val source = Source.fromFile(path_in)
    try {
      for (line <- source.getLines()) {
        val columns = line.split("\\|")
        try {
          val order = Order(
            o_orderkey = columns(0).toInt,
            o_custkey = columns(1).toInt,
            o_orderstatus = columns(2),
            o_totalprice = columns(3).toDouble,
            o_orderdate = columns(4),
            o_orderpriority = columns(5),
            o_clerk = columns(6),
            o_shippriority = columns(7).toInt,
            o_comment = columns(8)
          )
          result += order
        }catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
    } finally {
      source.close()
    }
    result
  }

  def ParsePart(filepath_in: String = "part.tbl"): ArrayBuffer[Part] = {
    val path_in: String = PATH + "TBL/" + filepath_in
    var result: ArrayBuffer[Part] = ArrayBuffer[Part]()
    val source = Source.fromFile(path_in)

    try {
      for (line <- source.getLines()) {
        val columns = line.split("\\|")
        try {
          val part = Part(
            p_partkey = columns(0).toInt,
            p_name = columns(1),
            p_mfgr = columns(2),
            p_brand = columns(3),
            p_type = columns(4),
            p_size = columns(5).toInt,
            p_container = columns(6),
            p_retailprice = columns(7).toDouble,
            p_comment = columns(8)
          )
          result += part
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
    } finally {
      source.close()
    }
    result
  }

  def ParsePartSupp(filepath_in: String = "partsupp.tbl"): ArrayBuffer[PartSupp] = {
    val path_in: String = PATH + "TBL/" + filepath_in
    var result: ArrayBuffer[PartSupp] = ArrayBuffer[PartSupp]()
    val source = Source.fromFile(path_in)

    try {
      for (line <- source.getLines()) {
        val columns = line.split("\\|")
        try {
          val partSupp = PartSupp(
            ps_partkey = columns(0).toInt,
            ps_suppkey = columns(1).toInt,
            ps_availqty = columns(2).toInt,
            ps_supplycost = columns(3).toDouble,
            ps_comment = columns(4)
          )
          result += partSupp
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
    } finally {
      source.close()
    }
    result
  }

  def ParseRegion(filepath_in: String = "region.tbl"): ArrayBuffer[Region] = {
    val path_in: String = PATH + "TBL/" + filepath_in
    var result: ArrayBuffer[Region] = ArrayBuffer[Region]()
    val source = Source.fromFile(path_in)

    try {
      for (line <- source.getLines()) {
        val columns = line.split("\\|")
        try {
          val region = Region(
            r_regionkey = columns(0).toInt,
            r_name = columns(1),
            r_comment = columns(2)
          )
          result += region
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
    } finally {
      source.close()
    }
    result
  }

  def ParseSupplier(filepath_in: String = "supplier.tbl"): ArrayBuffer[Supplier] = {
    val path_in: String = PATH + "TBL/" + filepath_in
    var result: ArrayBuffer[Supplier] = ArrayBuffer[Supplier]()
    val source = Source.fromFile(path_in)

    try {
      for (line <- source.getLines()) {
        val columns = line.split("\\|")
        try {
          val supplier = Supplier(
            s_suppkey = columns(0).toInt,
            s_name = columns(1),
            s_address = columns(2),
            s_nationkey = columns(3).toInt,
            s_phone = columns(4),
            s_acctbal = columns(5).toDouble,
            s_comment = columns(6)
          )
          result += supplier
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
    } finally {
      source.close()
    }
    result
  }


  // These classes are used for testing
  case class Contact(c_mobile:Int, c_email: String)extends JsonSerializable {
    override def toJson: JsValue = Json.toJson(this)(Json.writes[Contact])
  }
  object Contact{
    implicit val ContactFormat:Format[Contact] = Json.format[Contact]
  }

  case class Student(s_name: String, s_studentnumber:Int, s_contact:Contact)extends JsonSerializable {
    override def toJson: JsValue = Json.toJson(this)(Json.writes[Student])
  }
  object Student{
    implicit val StudentFormat:Format[Student] = Json.format[Student]
  }

  // This function generates a nested data table for testing
  def CreateNestedTest(): Unit ={
    var students: ArrayBuffer[Student] = ArrayBuffer[Student]()
    students += Student("Alice", 12345, Contact(123456789, "alice123@email.com"))
    students += Student("Bob", 54321 , Contact(987654321, "bob123@email.com"))
    WriteJson(students.map(_.asInstanceOf[JsonSerializable]), "student.json")
  }

  def WriteJson(elements:ArrayBuffer[JsonSerializable], filepath_out:String):Unit = {
    val json = Json.toJson(elements)
    val path_out = PATH + "JSON/"+ filepath_out
    val writer = new PrintWriter(new File(path_out))
    try{
      writer.write(Json.prettyPrint(json))
      println("JSON File Write To: " + path_out)
    } finally {
      writer.close()
    }
  }

  def main(args: Array[String]): Unit = {
    val customers = ParseCustomer().map(_.asInstanceOf[JsonSerializable])
    WriteJson(customers, "customer.json")
    val lineitems = ParseLineItem().map(_.asInstanceOf[JsonSerializable])
    WriteJson(lineitems, "lineitem.json")
    val nations = ParseNation().map(_.asInstanceOf[JsonSerializable])
    WriteJson(nations, "nation.json")
    val orders = ParseOrder().map(_.asInstanceOf[JsonSerializable])
    WriteJson(orders, "order.json")
    val parts = ParsePart().map(_.asInstanceOf[JsonSerializable])
    WriteJson(parts, "part.json")
    val partSupps = ParsePartSupp().map(_.asInstanceOf[JsonSerializable])
    WriteJson(partSupps, "partsupp.json")
    val regions = ParseRegion().map(_.asInstanceOf[JsonSerializable])
    WriteJson(regions, "region.json")
    val supplier = ParseSupplier().map(_.asInstanceOf[JsonSerializable])
    WriteJson(supplier, "supplier.json")
    CreateNestedTest()
  }

}
