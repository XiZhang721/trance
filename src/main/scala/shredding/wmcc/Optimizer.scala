package shredding.wmcc

import shredding.core._
import scala.collection.immutable.Set
import scala.collection.mutable.HashMap

object Optimizer {

  // temporary hack to handle missing label projections
  val tmp1 = HashMap[String, Set[String]]().withDefaultValue(Set())
  val tmp2 = HashMap[Variable, Set[String]]().withDefaultValue(Set())

  val proj = HashMap[Variable, Set[String]]().withDefaultValue(Set())
  var domainVars:List[Variable] = List()

  def getReferences(e: CExpr): Unit = e match {
    case LinearCSet(ps) => ps.foreach(p => println(getReferences(p)))
    case CNamed(n, p) => domainVars.foreach(p2 => { println(n); println(p2); println(p2.isReferenced(p)) })
    case _ => println("not a shredded plan")
  }
 
  def applyAll(e: CExpr) = {
    val p = mergeReduce(push(e))
    //println("looking at domain vars")
    //println(getReferences(p))
    p
  }

  def fields(e: CExpr):Unit = e match {
    case Record(ms) => ms.foreach(f => { 
      if (tmp1.contains(f._1) && f._2.isInstanceOf[Variable]){
        tmp2(f._2.asInstanceOf[Variable]) = tmp1(f._1)
      }
      fields(f._2)
    })
    case Project(Project(Project(_, "lbl"), s1), s2) => tmp1(s1) = Set(s2)
    case Project(v @ Variable(_,_), s) => 
      proj(v) = proj(v) ++ Set(s) ++ tmp2(v) 
    case _ => Unit//sys.error(s"unsupported $e")
  }

  def printhm():Unit = proj.foreach(f => println(s"${f._1.asInstanceOf[Variable].name} -> ${f._2}"))
  
  /**
    * Merge a reduce and a nest, this is necessary for returning the right answer for a nested
    * count comprehension that translates to reduceByKey(_+_)
    * unclear if this is spark specific or a bug in the unnest algorithm 
    */
  def mergeReduce(e: CExpr): CExpr = e match {
    case Reduce(n1 @ Nest( n2 @ Nest(e1, v3, f3, e3, v4, p3, g2), v1, Tuple(lbl), e2, v2, p2, g), v, f, p) if e3.tp.isInstanceOf[PrimitiveType] => e2 match {
      case Record(fs) =>
	val newgrps = fs.filter(_._2 != v4)
        val removed = fs.filterNot(_._2 != v4).keys.toList.head
	val newpat = newgrps.map{
			case (k,v) => v match {
			  case Project(v3, f) => (k, v3)
			  case _ => (k,v)
			} 
		     } ++ Map(removed -> v4)
        if (newgrps.nonEmpty) 
	  Reduce(Nest(Nest(e1, v3, Tuple(lbl ++ newgrps.values.toList), e3, v4, p3, g2), v1, Tuple(lbl), Record(newpat), v2, p2, g), v, f, p)
        else e
      case _ => e
    }
    case Reduce(Nest(e1, v1, f2, e2, v2, p2, g), v, f, p) => f match {
      case Record(fs) =>
	val newgrps = fs.filter(_._2 != v2)
        val removed = fs.filterNot(_._2 != v2).keys.toList.head
	val newpat = newgrps.map{
			case (k,v) => v match {
			  case Project(v3, f) => (k, v3)
			  case _ => (k,v)
			} 
		     } ++ Map(removed -> v2)
        if (newgrps.nonEmpty) 
	  Reduce(Nest(e1, v1, Tuple(newgrps.values.toList), e2, v2, p2, g), v, Record(newpat), p)
        else e
      case _ => e
    }
    case LinearCSet(exprs) => LinearCSet(exprs.map(mergeReduce(_)))
    case CNamed(n, e1) => CNamed(n, mergeReduce(e1))
    case _ => e
  }
  

  def push(e: CExpr): CExpr = e match {
    case Reduce(d, v, f, p) => 
      fields(f)
      Reduce(push(d), v, f, p)
    case Nest(e1, v1, f, e, v, p, g) => 
      fields(e)
      Nest(push(e1), v1, f, e, v, p, g)
    case Unnest(e1, v1, f, v2, p) =>
      fields(f)
      Unnest(push(e1), v1, f, v2, p)
    case OuterUnnest(e1, v1, f, v2, p) =>
      fields(f)
      OuterUnnest(push(e1), v1, f, v2, p)
    case Join(e1, e2, v1, p1, v2, p2) =>   
      fields(p1)
      fields(p2)
      Join(push(e1), push(e2), v1, p1, v2, p2)
    case OuterJoin(e1, e2, v1, p1, v2, p2) =>   
      fields(p1)
      fields(p2)
      OuterJoin(push(e1), push(e2), v1, p1, v2, p2)
    /**case Lookup(e1, e2, v1, p1, v2, p2, p3) => e // TODO
      fields(p2)
      fields(p3)
      Lookup(push(e1), push(e2), v1, p1, v2, p2, p3)
    case OuterLookup(e1, e2, v1, p1, v2, p2, p3) => e
      fields(p2)
      fields(p3)
      OuterLookup(push(e1), push(e2), v1, p1, v2, p2, p3)**/
    case Select(InputRef(n, _), v,_,_) if n.contains("M_ctx") => 
      domainVars = domainVars :+ v
      e 
    case Select(d, v, f, e2) =>
      fields(e)
      Select(push(d), v, f, Record((proj(v) ++ tmp2(v)).map(f2 => f2 -> Project(v, f2)).toMap))
    case CNamed(n, o) => CNamed(n, push(o))
    case LinearCSet(rs) => LinearCSet(rs.reverse.map(r => push(r)).reverse)
    case _ => e
  }

}
