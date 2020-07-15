package framework.plans

import framework.common._

/** Optimizer used for plans from BatchUnnester **/
object BatchOptimizer extends Extensions {

  val extensions = new Extensions{}
  import extensions._

  def applyAll(e: CExpr): CExpr = {
    val o1 = pushUnnest(e)
	val o2 = pushCondition(o1)
	val o3 = push(o2)
    o3
  }

  def validateMatch(t1: Type, f1: String, t2: Type, f2: String): Boolean = 
    (t1.attrs.get(f1).isDefined && t2.attrs.get(f2).isDefined) ||
      (t1.attrs.get(f2).isDefined && t2.attrs.get(f1).isDefined)

  /** TODO Yao's awesome optimizer **/
  def pushUnnest(e: CExpr): CExpr = fapply(e, {

    case DFOuterUnnest(
      AddIndex(DFOuterJoin(e1, x2, e2, x3, Constant(true), fs1), index),
        x7, field, x4, Equals(Project(x4_expr, f1), Project(x5, f2)), fs2)
          if validateMatch(x2.tp, f1, x4.tp, f2) => {

        //        if(x4.toString.equals(x4_expr.toString)){
        //      if(x2.tp.attrs.get(f1).isDefined && x4.tp.attrs.get(f2).isDefined){
        val unnest: DFUnnest = DFUnnest(
          AddIndex(e2, index), x3, field, x4, Constant(true), Nil)
        val cond = Equals(Project(x4_expr, f1), Project(x5, f2))
        DFOuterJoin(e1, x2, unnest, x7, cond, fs2)
    }

  })


  def pushCondition(e: CExpr): CExpr = fapply(e, {
	case DFProject(DFOuterJoin(e1, v1, e2, v2, Constant(true), fs1), v3,
		jc @ If(cond @ Equals(Project(_, f1), Project(_, f2)), s1, s2), fs2) =>
		DFProject(DFOuterJoin(e1, v1, e2, v2, cond, fs1), v3, 
			If(Equals(Project(v3, f2), Null),s1, s2), fs2)
  })

  /** Push projections in plans made of batch operations
    * @param e input plan from BatchUnnester
    * @param fs set of attributes, default empty set
    * @todo capture attributes from filter
    */
  def push(e: CExpr, fs: Set[String] = Set()): CExpr = e match {
    
    case DFProject(in, v, filter, fields) => 
      val tfields = fields.toSet ++ collect(filter)
      val pin = push(in, tfields ++ fs)
      val nv = Variable.fromBag(v.name, pin.tp)
      DFProject(pin, nv, replace(filter, nv), tfields.toList)

    case DFUnnest(in, v, path, v2, filter, fields) =>
      val pin = push(in, fields.toSet ++ fs + path)
      val nv = Variable.fromBag(v.name, pin.tp)
      DFUnnest(pin, nv, path, v2, filter, (fields.toSet ++ fs).toList)

    case DFOuterUnnest(in, v, path, v2, filter, fields) =>
      val pin = push(in, fields.toSet ++ fs + path)
      val nv = Variable.fromBag(v.name, pin.tp)
      DFOuterUnnest(pin, nv, path, v2, filter, (fields.toSet ++ fs).toList)

    case DFJoin(left, v, right, v2, cond, fields) =>
      val jcols = collect(cond)
      val nfields = fs ++ jcols
      val lpin = push(left, nfields) 
      val rpin = push(right, nfields)
      val lv = Variable.fromBag(v.name, lpin.tp)
      val rv = Variable.fromBag(v2.name, rpin.tp)
      DFJoin(lpin, lv, rpin, rv, cond, nfields.toList)

    case DFOuterJoin(left, v, right, v2, cond @ Equals(Project(_, p1), Project(_, p2 @ "_1")), fields) if right.tp.isDict =>
      // val jcols = collect(cond)
      val nfields = fs ++ Set(p1, p2)
      val lpin = push(left, nfields)
      val rpin = push(right, nfields)
      val lv = Variable.fromBag(v.name, lpin.tp)
      val rv = Variable.fromBag(v2.name, rpin.tp)
      println("right projections pushed")
      println(rpin)
      val nfields2 = if (nfields("_1")) nfields - p1 else nfields -- Set(p1, p2)
      DFOuterJoin(lpin, lv, rpin, rv, cond, nfields2.toList)

    case DFOuterJoin(left, v, right, v2, cond, fields) =>
      val jcols = collect(cond)
      val nfields = fs ++ jcols
      val lpin = push(left, nfields)
      val rpin = push(right, nfields)
      val lv = Variable.fromBag(v.name, lpin.tp)
      val rv = Variable.fromBag(v2.name, rpin.tp)
      DFOuterJoin(lpin, lv, rpin, rv, cond, nfields.toList)

    case DFNest(in, v, key, value, filter, nulls, ctag) => 
      // adjust key
      val indices = key.filter(k => k.contains("index")).toSet
      val nkey0 = (key.toSet & fs) ++ indices 
      val nkey = if (nkey0.isEmpty) key.toSet else nkey0

      val pfs = nkey ++ collect(value) ++ fs
      val pin = push(in, pfs)
      val nv = Variable.fromBag(v.name, pin.tp)
      DFNest(pin, nv, nkey.toList, replace(value, nv), filter, value.inputColumns.toList, ctag)

    case DFReduceBy(e1 @ DFProject(in, v, filter, fields), v2, key, value) =>
      // adjust key
      val indices = key.filter(k => k.contains("index")).toSet
      val nkey0 = (key.toSet & fs) ++ indices 
      val nkey = if (nkey0.isEmpty) key.toSet else nkey0

      val vs = nkey ++  value.toSet
      val nfilter = filter match {
    		case Record(ffs) => Record(ffs.filter(f => vs(f._1)))
    		case If(cond, Sng(Record(f1)), Some(Sng(Record(f2)))) => 
    			 If(cond, Sng(Record(f1.filter(f => vs(f._1)))), 
    			 	Some(Sng(Record(f2.filter(f => vs(f._1))))))
    		case _ => ???
	    } 
	    val nfs = vs ++ fs ++ collect(nfilter)
      val pin = push(in, nfs)
      val nv = Variable.fromBag(v.name, pin.tp)

      val pin2 = DFProject(pin, nv, nfilter, nfs.toList)
      val nv2 = Variable.fromBag(v2.name, pin2.tp)
      DFReduceBy(pin2, nv2, nkey.toList, value)

    case DFReduceBy(in, v, key, value) =>
      //adjust key
      val indices = key.filter(k => k.contains("index")).toSet
      val nkey = (key.toSet & fs) ++ indices

      val pin = push(in, nkey ++ value.toSet ++ fs)
      val nv = Variable.fromBag(v.name, pin.tp)
      DFReduceBy(pin, nv, nkey.toList, value)

    case Select(in, v, p, v2:Variable) =>
      val ptp = v.tp.attrs.filter(f => fs(f._1))
      val nv = Variable(v2.name, RecordCType(ptp))
      Select(in, v, p, nv)

    case CGet(e1) => CGet(push(e1, fs))
    case AddIndex(e1, name) => AddIndex(push(e1, fs), name)
    case FlatDict(e1) => FlatDict(push(e1, fs))
    case GroupDict(e1) => GroupDict(push(e1, fs))
    case CNamed(n, e1) => CNamed(n, push(e1))
    case LinearCSet(fs) => LinearCSet(fs.map(f => push(f)))
    case InputRef(name, tp) => 
      val fields = fs & tp.attrs.keySet
      if (fields.nonEmpty) {
        val v = Variable.fresh(RecordCType(tp.attrs))
        val nv = Variable(v.name, RecordCType(tp.attrs.filter(f => fields(f._1))))
        Select(e, v, Constant(true), nv)
      } else InputRef(name, tp)
    case _ => e
  }

}
