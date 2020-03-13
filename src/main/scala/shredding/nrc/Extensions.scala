package shredding.nrc

import shredding.core._

/**
  * Extension methods for NRC expressions
  */
trait Extensions {
  this: MaterializeNRC with Factory with Implicits =>

  def collect[A](e: Expr, f: PartialFunction[Expr, List[A]]): List[A] =
    f.applyOrElse(e, (ex: Expr) => ex match {
      case p: Project =>
        collect(p.tuple, f)
      case ForeachUnion(_, e1, e2) =>
        collect(e1, f) ++ collect(e2, f)
      case Union(e1, e2) =>
        collect(e1, f) ++ collect(e2, f)
      case Singleton(e1) =>
        collect(e1, f)
      case DeDup(e1) =>
        collect(e1, f)
      case Tuple(fs) =>
        fs.flatMap(x => collect(x._2, f)).toList
      case l: Let =>
        collect(l.e1, f) ++ collect(l.e2, f)
      case c: Cmp =>
        collect(c.e1, f) ++ collect(c.e2, f)
      case And(e1, e2) =>
        collect(e1, f) ++ collect(e2, f)
      case Or(e1, e2) =>
        collect(e1, f) ++ collect(e2, f)
      case Not(e1) =>
        collect(e1, f)
      case i: IfThenElse =>
        collect(i.cond, f) ++ collect(i.e1, f) ++ i.e2.map(collect(_, f)).getOrElse(Nil)
      case ArithmeticExpr(_, e1, e2) =>
        collect(e1, f) ++ collect(e2, f)
      case Count(e1) =>
        collect(e1, f)
      case Sum(e1, _) =>
        collect(e1, f)
      case GroupByKey(e, _, _) =>
        collect(e, f)
      case SumByKey(e, _, _) =>
        collect(e, f)

      // Label extensions
      case l: ExtractLabel =>
        collect(l.lbl, f) ++ collect(l.e, f)
      case l: NewLabel =>
        l.params.toList.flatMap(collect(_, f))
      case p: LabelParameter =>
        collect(p.e, f)

      // Dictionary extensions
      case BagDict(_, b, d) =>
        collect(b, f) ++ collect(d, f)
      case TupleDict(fs) =>
        fs.flatMap(x => collect(x._2, f)).toList
      case TupleDictProject(d) =>
        collect(d, f)
      case d: DictUnion =>
        collect(d.dict1, f) ++ collect(d.dict2, f)

      // Shredding extensions
      case ShredUnion(e1, e2) =>
        collect(e1, f) ++ collect(e2, f)
      case Lookup(l, d) =>
        collect(l, f) ++ collect(d, f)

      // Materialization extensions
      case MatDictLookup(l, b) =>
        collect(l, f) ++ collect(b, f)

      case _ => List()
    })

  def collectAssignment[A](a: Assignment, f: PartialFunction[Expr, List[A]]): List[A] =
    collect(a.rhs, f)

  def collectProgram[A](p: Program, f: PartialFunction[Expr, List[A]]): List[A] =
    p.statements.flatMap(collectAssignment(_, f))

  def replace(e: Expr, f: PartialFunction[Expr, Expr]): Expr =
    f.applyOrElse(e, (ex: Expr) => ex match {
      case p: Project =>
        Project(replace(p.tuple, f).asInstanceOf[AbstractTuple], p.field)
      case ForeachUnion(x, e1, e2) =>
        val r1 = replace(e1, f).asInstanceOf[BagExpr]
        val xd = VarDef(x.name, r1.tp.tp)
        val r2 = replace(e2, f).asInstanceOf[BagExpr]
        ForeachUnion(xd, r1, r2)
      case Union(e1, e2) =>
        val r1 = replace(e1, f).asInstanceOf[BagExpr]
        val r2 = replace(e2, f).asInstanceOf[BagExpr]
        Union(r1, r2)
      case Singleton(e1) =>
        Singleton(replace(e1, f).asInstanceOf[TupleExpr])
      case DeDup(e1) =>
        DeDup(replace(e1, f).asInstanceOf[BagExpr])
      case Tuple(fs) =>
        Tuple(fs.map(x => x._1 -> replace(x._2, f).asInstanceOf[TupleAttributeExpr]))
      case l: Let =>
        val r1 = replace(l.e1, f)
        val xd = VarDef(l.x.name, r1.tp)
        val r2 = replace(l.e2, f)
        Let(xd, r1, r2)
      case c: Cmp =>
        val c1 = replace(c.e1, f).asInstanceOf[PrimitiveExpr]
        val c2 = replace(c.e2, f).asInstanceOf[PrimitiveExpr]
        Cmp(c.op, c1, c2)
      case And(e1, e2) =>
        val c1 = replace(e1, f).asInstanceOf[CondExpr]
        val c2 = replace(e2, f).asInstanceOf[CondExpr]
        And(c1, c2)
      case Or(e1, e2) =>
        val c1 = replace(e1, f).asInstanceOf[CondExpr]
        val c2 = replace(e2, f).asInstanceOf[CondExpr]
        Or(c1, c2)
      case Not(e1) =>
        Not(replace(e1, f).asInstanceOf[CondExpr])
      case i: IfThenElse =>
        val c = replace(i.cond, f).asInstanceOf[CondExpr]
        val r1 = replace(i.e1, f)
        if (i.e2.isDefined)
          IfThenElse(c, r1, replace(i.e2.get, f))
        else
          IfThenElse(c, r1)
      case ArithmeticExpr(op, e1, e2) =>
        val n1 = replace(e1, f).asInstanceOf[NumericExpr]
        val n2 = replace(e2, f).asInstanceOf[NumericExpr]
        ArithmeticExpr(op, n1, n2)
      case Count(e1) =>
        Count(replace(e1, f).asInstanceOf[BagExpr])
      case Sum(e1, fs) =>
        Sum(replace(e1, f).asInstanceOf[BagExpr], fs)
      case GroupByKey(e1, ks, vs) =>
        GroupByKey(replace(e1, f).asInstanceOf[BagExpr], ks, vs)
      case SumByKey(e1, ks, vs) =>
        SumByKey(replace(e1, f).asInstanceOf[BagExpr], ks, vs)

      // Label extensions
      case x: ExtractLabel =>
        val rl = replace(x.lbl, f).asInstanceOf[LabelExpr]
        val re = replace(x.e, f)
        ExtractLabel(rl, re)
      case l: NewLabel =>
        NewLabel(l.params.map(replace(_, f).asInstanceOf[LabelParameter]), l.id)
      case VarRefLabelParameter(v) =>
        VarRefLabelParameter(replace(v, f).asInstanceOf[Expr with VarRef])
      case ProjectLabelParameter(p) =>
        ProjectLabelParameter(replace(p, f).asInstanceOf[Expr with Project])

      // Dictionary extensions
      case BagDict(tp, b, d) =>
        val rb = replace(b, f).asInstanceOf[BagExpr]
        val rd = replace(d, f).asInstanceOf[TupleDictExpr]
        BagDict(tp, rb, rd)
      case TupleDict(fs) =>
        TupleDict(fs.map(x => x._1 -> replace(x._2, f).asInstanceOf[TupleDictAttributeExpr]))
      case TupleDictProject(d) =>
        replace(d, f).asInstanceOf[BagDictExpr].tupleDict
      case d: DictUnion =>
        val r1 = replace(d.dict1, f).asInstanceOf[DictExpr]
        val r2 = replace(d.dict2, f).asInstanceOf[DictExpr]
        DictUnion(r1, r2)

      // Shredding extensions
      case ShredUnion(e1, e2) =>
        val r1 = replace(e1, f).asInstanceOf[BagExpr]
        val r2 = replace(e2, f).asInstanceOf[BagExpr]
        ShredUnion(r1, r2)
      case Lookup(l, d) =>
        val rl = replace(l, f).asInstanceOf[LabelExpr]
        val rd = replace(d, f).asInstanceOf[BagDictExpr]
        rd.lookup(rl)

      // Materialization extensions
      case MatDictLookup(l, b) =>
        val rl = replace(l, f).asInstanceOf[LabelExpr]
        val rb = replace(b, f).asInstanceOf[BagExpr]
        MatDictLookup(rl, rb)

      case _ => ex
    })

  def replaceAssignment(a: Assignment, f: PartialFunction[Expr, Expr]): Assignment =
    Assignment(a.name, replace(a.rhs, f))

  def replaceProgram(p: Program, f: PartialFunction[Expr, Expr]): Program =
    Program(p.statements.map(replaceAssignment(_, f)))

  // Replace label parameter projections with variable references
  def createLambda(lbl: NewLabel, e: BagExpr): BagExpr =
    lbl.params.foldRight(e) {
      case (l: ProjectLabelParameter, acc) =>
        projectionToVar(acc, l).asInstanceOf[BagExpr]
      case (_, acc) =>
        acc
    }

  // Replace projection with a variable reference
  protected def projectionToVar(e: Expr, l: ProjectLabelParameter): Expr = replace(e, {
    case p: Project if p.tuple.name == l.e.tuple.name && p.field == l.e.field =>
      // Sanity check
      assert(p.tp == l.tp, "[projectionToVar] Types differ: " + p.tp + " and " + l.tp)
      VarRef(l.name, l.tp)
    case p: ProjectLabelParameter if p.name == l.name =>
      // Sanity check
      assert(p.tp == l.tp, "[projectionToVar] Types differ: " + p.tp + " and " + l.tp)
      VarRefLabelParameter(VarRef(l.name, l.tp).asInstanceOf[Expr with VarRef])
  })

  // Replace variable references with projections
  def applyLambda(lbl: NewLabel, e: BagExpr): BagExpr =
    lbl.params.foldRight(e) {
      case (l: ProjectLabelParameter, acc) =>
        varToProjection(acc, l).asInstanceOf[BagExpr]
      case (_, acc) =>
        acc
    }

  // Replace variable reference with a projection
  protected def varToProjection(e: Expr, l: ProjectLabelParameter): Expr = replace(e, {
    case v: VarRef if v.name == l.name =>
      // Sanity check
      assert(v.tp == l.tp, "[varToProjection] Types differ: " + v.tp + " and " + l.tp)
      l.e
    case p: VarRefLabelParameter if p.name == l.name =>
      // Sanity check
      assert(p.tp == l.tp, "[varToProjection] Types differ: " + p.tp + " and " + l.tp)
      l
  })

  def labelParameters(e: Expr): Set[LabelParameter] = 
    labelParameters(e, Map.empty).toSet //filterNot(invalidLabelElement).toSet

  protected def labelParameters(e: Expr, scope: Map[String, VarDef]): List[LabelParameter] = collect(e, {
    case v: VarRef =>
      filterByScope(v, scope).map(_ => VarRefLabelParameter(v)).toList
    case p: Project =>
      filterByScope(p.tuple, scope).map(_ => ProjectLabelParameter(p)).toList
    case ForeachUnion(x, e1, e2) =>
      labelParameters(e1, scope) ++ labelParameters(e2, scope + (x.name -> x))
    case l: Let =>
      labelParameters(l.e1, scope) ++ labelParameters(l.e2, scope + (l.x.name -> l.x))
    case p: VarRefLabelParameter =>
      filterByScope(p.e, scope).map(_ => p).toList
    case p: ProjectLabelParameter =>
      filterByScope(p.e.tuple, scope).map(_ => p).toList
    case BagDict(ltp, f, d) =>
      val params = ltp.attrTps.map(v => v._1 -> VarDef(v._1, v._2))
      labelParameters(f, scope ++ params) ++ labelParameters(d, scope)
  })

  // Input labels and dictionaries are invalid in labels
  protected def invalidLabelElement(e: LabelParameter): Boolean = e match {
    case VarRefLabelParameter(v: LabelVarRef) => v.tp.attrTps.isEmpty
    case _ => e.tp.isInstanceOf[DictType]
  }

  protected def filterByScope(v: VarRef, scope: Map[String, VarDef]): Option[VarRef] =
    scope.get(v.name) match {
      case Some(v2) =>
        // Sanity check
        assert(v.tp == v2.tp, "[filterByScope] Types differ: " + v.tp + " and " + v2.tp)
        None
      case None => Some(v)
    }

  def inputVars(e: Expr): Set[VarRef] =
    inputVars(e, Map.empty[String, VarDef]).toSet

  def inputVars(a: Assignment): Set[VarRef] =
    inputVars(a.rhs)

  def inputVars(p: Program): Set[VarRef] =
    p.statements.foldLeft (Map.empty[String, VarDef], Set.empty[VarRef]) {
      case ((scope, ivars), s) =>
        (scope + (s.name -> VarDef(s.name, s.rhs.tp)),
          ivars ++ inputVars(s.rhs, scope).toSet)
    }._2

  def inputVars(e: ShredExpr): Set[VarRef] =
    inputVars(e, Map.empty[String, VarDef]).toSet

  def inputVars(a: ShredAssignment): Set[VarRef] =
    inputVars(a, Map.empty[String, VarDef])

  def inputVars(p: ShredProgram): Set[VarRef] =
    p.statements.foldLeft (Map.empty[String, VarDef], Set.empty[VarRef]) {
      case ((scope, ivars), s) =>
        ( scope ++ Map(
          flatName(s.name) -> VarDef(flatName(s.name), s.rhs.flat.tp),
          dictName(s.name) -> VarDef(dictName(s.name), s.rhs.dict.tp)
        ),
          ivars ++ inputVars(s.rhs, scope).toSet )
    }._2

  protected def inputVars(e: Expr, scope: Map[String, VarDef]): List[VarRef] = collect(e, {
    case v: VarRef => filterByScope(v, scope).toList
    case ForeachUnion(x, e1, e2) =>
      inputVars(e1, scope) ++ inputVars(e2, scope + (x.name -> x))
    case l: Let =>
      inputVars(l.e1, scope) ++ inputVars(l.e2, scope + (l.x.name -> l.x))
    case BagDict(ltp, f, d) =>
      val params = ltp.attrTps.map(v => v._1 -> VarDef(v._1, v._2))
      inputVars(f, scope ++ params) ++ inputVars(d, scope)
  })

  protected def inputVars(a: Assignment, scope: Map[String, VarDef]): List[VarRef] =
    inputVars(a.rhs, scope)

  protected def inputVars(e: ShredExpr, scope: Map[String, VarDef]): List[VarRef] =
    inputVars(e.flat, scope) ++ inputVars(e.dict, scope)

  protected def inputVars(a: ShredAssignment, scope: Map[String, VarDef]): Set[VarRef] =
    inputVars(a.rhs.flat, scope).toSet ++ inputVars(a.rhs.dict, scope).toSet

}
