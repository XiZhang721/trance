package shredding.nrc

import shredding.core.{VarDef, TupleType, OpEq}
import shredding.utils.Utils.Symbol

/**
  * Simple optimizations:
  *
  *   - let inlining:
  *         let X = e1 in X => e1
  *
  *   - dead code elimination
  *         let X = e1 in e2 => e2 if not using X
  *
  */
trait Optimizer extends Extensions with Implicits with Factory {
  this: MaterializeNRC =>

  def optimize(p: ShredProgram): ShredProgram =
    ShredProgram(p.statements.map(optimize))

  def optimize(a: ShredAssignment): ShredAssignment =
    ShredAssignment(a.name, optimize(a.rhs))

  def optimize(e: ShredExpr): ShredExpr =
    ShredExpr(optimize(e.flat), optimize(e.dict).asInstanceOf[DictExpr])

  def optimize(p: Program): Program =
    Program(p.statements.map(optimize))

  def optimize(a: Assignment): Assignment =
    Assignment(a.name, optimize(a.rhs))

  def optimize(e: Expr): Expr = inline(deadCodeElimination(e))

  def inline(e: Expr): Expr = replace(e, {
    // let X = e1 in X => e1
    case l: Let if (l.e2 match {
      case v2: VarRef if v2.varDef == l.x => true
      case _ => false
    }) => inline(l.e1)
  })

  def deadCodeElimination(e: Expr): Expr = replace(e, {
    // let X = e1 in e2 => e2 if not using X
    case l: Let if {
      val im = inputVars(l.e2).map(v => v.name -> v.tp).toMap
      // Sanity check
      im.get(l.x.name).foreach { tp =>
        assert(tp == l.x.tp,
          "[deadCodeElimination] Type differs " + tp + " and " + l.x.tp)
      }
      !im.contains(l.x.name)
    } => deadCodeElimination(l.e2)
  })

  def nestingRewrite(e: Expr): Expr = replace(e, {
    case f @ ForeachUnion(x, b1,
      BagIfThenElse(
        PrimitiveCmp(OpEq,
          p1 @ PrimitiveProject(t1: TupleVarRef, f1),
          p2 @ PrimitiveProject(t2: TupleVarRef, f2)),
        b2, None)) =>

      val bag1 = nestingRewrite(b1).asInstanceOf[BagExpr]
      val bag2 = nestingRewrite(b2).asInstanceOf[BagExpr]

      val ivars = inputVars(f)
      if (ivars.contains(t1) && !ivars.contains(t2)) {
        val gb = VarDef(Symbol.fresh("gb"), TupleType("key" -> t2.tp(f2), "value" -> bag2.tp))
        val gbr = TupleVarRef(gb)
        ForeachUnion(gb,
          ForeachUnion(x, bag1, Singleton(Tuple("key" -> p2, "value" -> bag2))),
          BagIfThenElse(Cmp(OpEq, p1, gbr("key")), gbr("value").asInstanceOf[BagExpr], None)
        )
      }
      else if (!ivars.contains(t1) && ivars.contains(t2)) {
        val gb = VarDef(Symbol.fresh("gb"), TupleType("key" -> t1.tp(f1), "value" -> bag2.tp))
        val gbr = TupleVarRef(gb)
        ForeachUnion(gb,
          ForeachUnion(x, bag1, Singleton(Tuple("key" -> p1, "value" -> bag2))),
          BagIfThenElse(Cmp(OpEq, p2, gbr("key")), gbr("value").asInstanceOf[BagExpr], None)
        )
      }
      else ForeachUnion(x, bag1, BagIfThenElse(Cmp(OpEq, p1, p2), bag2, None))
    case f @ ForeachUnion(x, b1, BagIfThenElse(And(cond1, cond2), b2, None)) => cond1 match {
      case PrimitiveCmp(OpEq, p1 @ PrimitiveProject(t1: TupleVarRef, f1), p2 @ PrimitiveVarRef(t2)) =>
        rewriteJoinOnLabel(inputVars(f), x, b1, 
          BagIfThenElse(cond2, nestingRewrite(b2).asInstanceOf[BagExpr], None), p1, t1, p2, p2) 
      case _ => f
    }
      
  })

  def rewriteJoinOnLabel(ivars: Set[VarRef], x: VarDef, b1: Expr, b2: Expr, p1: TupleAttributeExpr, t1: VarRef, p2: Expr, t2: VarRef): Expr = {
    val bag1 = nestingRewrite(b1).asInstanceOf[BagExpr]
    val bag2 = nestingRewrite(b2).asInstanceOf[BagExpr]

    if (ivars.contains(t1) && !ivars.contains(t2)) {
      ForeachUnion(x, bag1, Singleton(Tuple("key" -> 
        NewLabel(Set(VarRefLabelParameter(p2.asInstanceOf[Expr with VarRef]))), "value" -> bag2)))
    }
    else if (!ivars.contains(t1) && ivars.contains(t2)) {
      // substitute the new projection 
      // with the variable name in the other label
      // so that their record types will later match
      val vd = VarDef(p2.asInstanceOf[PrimitiveVarRef].varDef.name, p1.tp)
      val label = NewLabel(Set(VarRefLabelParameter(PrimitiveVarRef(vd))))
      ForeachUnion(x, bag1, BagLet(vd, p1, Singleton(Tuple("key" -> 
        label.asInstanceOf[TupleAttributeExpr], "value" -> bag2))))
    }
    else ForeachUnion(x, bag1, BagIfThenElse(Cmp(OpEq, 
        p1, p2.asInstanceOf[TupleAttributeExpr]), bag2, None))
  }

  // could validate right singleton with attribute names
  def embedKey(key: Expr, e: Expr): Expr = replace(e, {
    case Singleton(Tuple(fs)) => 
      Singleton(Tuple(Map("key" -> key.asInstanceOf[TupleAttributeExpr]) ++ fs))
  })

  /** 
    * //TODO needs to be generic enough to extract label match from and conditions 
    * 
    */
  def nestingRewriteLossy(e: Expr): Expr = replace(e, { 
    case f @ ForeachUnion(x, b1, BagIfThenElse(cond, b2, None)) => cond match {
      /**case Cmp(OpEq, p1 @ PrimitiveProject(t1: TupleVarRef, f1), p2 @ PrimitiveProject(t2: TupleVarRef, f2)) =>
        rewriteJoinOnLabel(inputVars(f), x, b1, b2, p1, t1, p2, t2)**/
      case PrimitiveCmp(OpEq, p1 @ PrimitiveProject(t1: TupleVarRef, f1), p2 @ PrimitiveVarRef(t2)) =>
        rewriteJoinOnLabel(inputVars(f), x, b1, b2, p1, t1, p2, p2)
      case PrimitiveCmp(OpEq, p2 @ PrimitiveVarRef(t2), p1 @ PrimitiveProject(t1: TupleVarRef, f1)) =>
        rewriteJoinOnLabel(inputVars(f), x, b1, b2, p1, t1, p2, p2)
      case And(PrimitiveCmp(OpEq, p1 @ PrimitiveProject(t1: TupleVarRef, f1), p2 @ PrimitiveVarRef(t2)), cond2) =>
        rewriteJoinOnLabel(inputVars(f), x, b1, 
          BagIfThenElse(cond2, nestingRewrite(b2).asInstanceOf[BagExpr], None), p1, t1, p2, p2) 
      case And(PrimitiveCmp(OpEq, p2 @ PrimitiveVarRef(t2), p1 @ PrimitiveProject(t1: TupleVarRef, f1)), cond2) =>
        rewriteJoinOnLabel(inputVars(f), x, b1, 
          BagIfThenElse(cond2, nestingRewrite(b2).asInstanceOf[BagExpr], None), p1, t1, p2, p2) 
      case _ => f
    }
  })
}
