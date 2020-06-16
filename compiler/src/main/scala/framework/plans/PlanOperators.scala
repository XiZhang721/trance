package framework.plans

import framework.common._

/** Batch processing operators **/

case class AddIndex(e: CExpr, name: String) extends CExpr {
  def tp: BagCType = BagCType(RecordCType(e.tp.attrs ++ Map(name -> LongType)))
}

// rename filter
case class DFProject(in: CExpr, v: Variable, filter: CExpr, fields: List[String]) extends CExpr {
  
  override def inputColumns: Set[String] = v.tp.attrs.keySet

  def rename: Map[String, String] = filter match {
    case Record(fs) => fs.toList.flatMap{
      case (key, Project(_, fp)) if key != fp => List((key, fp))
      case (key, Label(fs)) => fs.head match {
        case (_, Project(_, fp)) if key != fp => List((key, fp))
        case _ => Nil
      }
      case _ => Nil
    }.toMap
    case _ => Map()
  }

  def makeCols: Map[String, CExpr] = filter match {
    case r:Record => 
      val fields = r.inputColumns ++ rename.keySet
      r.fields.filter(f => !fields(f._1))
    case _ => Map()
  }

  def tp: BagCType = BagCType(filter.tp)
}

case class DFUnnest(in: CExpr, v: Variable, path: String, v2: Variable, filter: CExpr, fields: List[String]) extends CExpr {
  def tp: BagCType = BagCType(v.tp.merge(v2.tp).project(fields))
}

case class DFOuterUnnest(in: CExpr, v: Variable, path: String, v2: Variable, filter: CExpr, fields: List[String]) extends CExpr {
  val index = Map(path+"_index" -> LongType)
  def tp: BagCType = 
    BagCType(RecordCType((v.tp.attrs - path) ++ index).merge(v2.tp.outer).project(fields))
}

/** Join operators **/

trait JoinOp extends CExpr {

  def tp: BagCType

  val left: CExpr
  val v: Variable

  val right: CExpr
  val v2: Variable

  val cond: CExpr

  val fields: List[String]
  val jtype: String

  val isEquiJoin: Boolean = cond match {
    case Equals(_:Project,_:Project) => true
    case _ => false
  }

}

case class DFJoin(left: CExpr, v: Variable, right: CExpr, v2: Variable, cond: CExpr, fields: List[String]) extends JoinOp {
  def tp: BagCType = BagCType(v.tp.merge(v2.tp).project(fields))
  val jtype = "inner"
}

case class DFOuterJoin(left: CExpr, v: Variable, right: CExpr, v2: Variable, cond: CExpr, fields: List[String]) extends JoinOp {
  def tp: BagCType = BagCType(v.tp.merge(v2.tp.outer).project(fields))
  val jtype = "left_outer"
}

case class DFNest(in: CExpr, v: Variable, key: List[String], value: CExpr, filter: CExpr, nulls: List[String], ctag: String) extends CExpr {
  def tp: BagCType = value.tp match {
    case _:NumericType => BagCType(RecordCType(v.tp.project(key).attrTps ++ Map(ctag -> DoubleType)))
    case _ => BagCType(RecordCType(v.tp.project(key).attrTps ++ Map(ctag -> BagCType(value.tp.unouter))))
  }
}

case class DFReduceBy(in: CExpr, v: Variable, keys: List[String], values: List[String]) extends CExpr {
  def tp: BagCType = BagCType(v.tp.project(keys).merge(v.tp.project(values)))
}
