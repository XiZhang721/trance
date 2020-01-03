
package experiments
/** 
This is manually written code based on the experiments
provided from Slender.s
**/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._

object Query4New4SparkManual {
  def main(args: Array[String]){
    val sf = Config.datapath.split("/").last
    val conf = new SparkConf().setMaster(Config.master).setAppName("Query4New4SparkManual"+sf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val tpch = TPCHLoader(spark)
    val C = tpch.loadCustomers
    C.cache
    C.count
    val O = tpch.loadOrders
    O.cache
    O.count
    val L = tpch.loadLineitem
    L.cache
    L.count
    val P = tpch.loadPart
    P.cache
    P.count
       

    val l = L.map(l => l.l_partkey -> (l.l_orderkey, l.l_quantity))
    val p = P.map(p => p.p_partkey -> p.p_name)
    val lpj = l.joinSkewLeft(p)

    val OrderParts = lpj.map{ case (_, ((l_orderkey, l_quantity), p_name)) => (l_orderkey, (p_name, l_quantity)) }.groupByKey()
    val o = O.map(o => o.o_orderkey -> (o.o_custkey, o.o_orderdate)).join(OrderParts)

    val CustomerOrders = o.map{ case (_, ((o_custkey, o_orderdate), parts)) => (o_custkey, (o_orderdate, parts)) }.groupByKey()
    val c = C.map(c => c.c_custkey -> c.c_name).join(CustomerOrders).map{ case (_, (c_name, orders)) => (c_name, orders) }
    c.count
    var start0 = System.currentTimeMillis()
	  val result = c.flatMap{ 
      case (cname, orders) => orders.flatMap{ case (date, parts) =>
        parts.map{ case (part, qty) => cname -> part }
      }
    }.join(C.map(c => c.c_name -> c.c_address)).map{
      case (cname, (part, address)) => part -> (cname, address)
    }.groupByKey()
	  result.count
  var end0 = System.currentTimeMillis() - start0
    println("Query4New4SparkManual"+sf+","+Config.datapath+","+end0)
  }
}
