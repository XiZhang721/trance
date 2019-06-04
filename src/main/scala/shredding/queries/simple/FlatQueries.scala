package shredding.queries.simple

import shredding.core._
import shredding.nrc.LinearizedNRC

object FlatTests {
  
  val nrc = new LinearizedNRC{}
  val data1 = List(Map("a" -> 42, "b" -> "Milos"), Map("a" -> 49, "b" -> "Michael"),
                           Map("a" -> 34, "b" -> "Jaclyn"), Map("a" -> 42, "b" -> "Thomas"))
  
  val query1 = {
    import nrc._
    val dtype = TupleType("a" -> IntType, "b" -> StringType)
    val relR = BagVarRef(VarDef("R", BagType(dtype)))
    val x = VarDef("x", dtype)
    ForeachUnion(x, relR, Singleton(Tuple("o1" -> TupleVarRef(x)("a"))))
  }


}

