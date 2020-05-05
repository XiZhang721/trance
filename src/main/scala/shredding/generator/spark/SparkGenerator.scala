package shredding.generator.spark

import shredding.core._
import shredding.plans.{Multiply => CMultiply}
import shredding.plans._
import shredding.utils.Utils.ind

/**
  * Spark/Scala generator for RDD. This was the stable Spark generator 
  * before Datasets.
  * @deprecated no longer supported, see dataset generator
  * @param cache boolean flag for caching intermediate outputs
  * @param evaluate boolean flag for evaluating intermediate outputs
  * @param skew boolean flag for skew-aware application (requires only minor adjustments)
  * @param flatDict boolean flag for relational representation of dictionaries; used prior to 
  * the development of the dataset generator
  * @param inputs map of input types that should not be reproduced (important for programs)
  */
class SparkNamedGenerator(cache: Boolean, evaluate: Boolean, skew: Boolean = false,
  flatDict: Boolean = false, inputs: Map[Type, String] = Map()) extends SparkTypeHandler with SparkUtils {

  implicit def expToString(e: CExpr): String = generate(e)

  var types: Map[Type, String] = inputs

  /** Generates the code for the set of case class records associated to the 
    * records in the generated program.
    *
    * @param names list of names to omit from header creation
    * @return string representing all the records required to run the corresponding application
    */
  def generateHeader(names: List[String] = List()): String = {
    val h1 = typelst.map(x => generateTypeDef(x)).mkString("\n")
    val h2 = inputs.withFilter(x => !names.contains(x._2)).map( x => generateTypeDef(x._1)).toList
    if (h2.nonEmpty) { s"$h1\n${h2.mkString("\n")}" } else { h1 }
  }

  /** Generate a conditional expression 
    * 
    * @param p condition
    * @param thenp string representing generated code if p is true
    * @param elsep string representing generated code if p is false
    * @return string of the code corresponding to the conditional expression
    */
  private def conditional(p: CExpr, thenp: String, elsep: String): String = p match {
    case Constant(true) => s"${ind(thenp)}"
    case _ => s"if({${generate(p)}}) {${ind(thenp)}} else {${ind(elsep)}}"
  }

  /** Check if a variable is null before projecting on it. 
    * Necessary for null values introduced from the standard pipeline.
    * 
    * @param e expression containing projection
    * @param grouped boolean flag for putting null values inside records
    * @return string representing the null checks for expressions that include projections
    */
  private def nullProject(e: CExpr, grouped: Boolean = false): String = e match {
    case Bind(bv, p1 @ Project(v, field), Bind(bv2, p2 @ Record(_), p3)) if grouped =>
      s"""|{val ${generate(bv2)} = ${generate(v)} match {
          |   case null => ${castNull(p1)}; case _ => {
          |     val ${generate(bv)} = ${generate(v)}.$field
          |     ${generate(p2)} }}
          |${generate(p3)}}""".stripMargin
    case Bind(v, Project(v2, "_1"), e2) => generate(Bind(v, v2, e2))
    case Bind(bv, p @ Project(e1, field), e2) => 
      s"""|val ${generate(bv)} = ${generate(e1)} match { 
          | case null => ${castNull(p)}; case pv => pv.${kvName(field)} }
          |${nullProject(e2, grouped)}""".stripMargin
    case Bind(v, t:Tuple, proj) => 
      s"val ${generate(v)} = ${generate(t)}\n${nullProject(proj, grouped)}"
    case Project(v, field) => 
      s"${generate(v)} match { case null => ${castNull(e)}; case pv => pv.${kvName(field)} }"
    case _ => generate(e)
  }

  /** Helper function for dropping the bag attribute for the unnest operator, 
    * this also controls introducing the index.
    *
    * @param e input record containing the path attribute
    * @param vs list of variables corresponding to the left portition of the plan
    * @param index boolean flag to produce index or not
    * @return a set of information required for the indexed unnested bag record
    * and the parent record that has the unnestd bag attribute removed.
    */
  private def projectBag(e: CExpr, vs: List[Variable], index: Boolean = true): (String, String, List[CExpr], List[CExpr], CExpr) = e match {
    case Bind(v, Project(v2 @ Variable(n,tp), field), e2) => 
      val nvs1 = vs.map( v3 => if (v3 == v2) drop(tp, v2, field, index) else v3)
      val nvs2 = vs.map( v3 => if (v3 == v2) v2.nullValue else v3)
      (n, field, nvs1, nvs2, e2)
    case _ => sys.error(s"unsupported bag projection $e")
  }

  /** Drops the bag attribute from a record and places the index if necessary.
    *
    * @param tp type of the input record to drop the bag attribute from
    * @param v varibale to place in the attributes of the new record
    * @param field the field corresponding to the bag attribute to be dropped
    * @param index boolean flag to produce index or not
    * @return record with the bag attribute dropped and an index introduced
    */
  private def drop(tp: Type, v: Variable, field: String, index: Boolean = true): CExpr = tp match {
      case TTupleType(fs) => 
        Tuple(fs.drop((kvName(field)(2).replace("_", "").toInt-1)).zipWithIndex.map{ case (t, i) 
          => Project(v, "_"+ (i+1)) })
      case RecordCType(fs) => 
        val imap = if (index) Map("index" -> Index) else Map()
        Record(imap ++ (fs - field).map{ case (
          attr, atp) => attr -> Project(v, attr)})
      case MatDictCType(lbl, BagCType(r @ RecordCType(fs))) => 
        val nv = Variable(v.name, r)
        Record(fs.map{ case (attr, atp) => attr -> Project(nv, attr)})
      case _ => sys.error(s"unsupported type ${tp}")
    }

  /** Main generator for Spark applications using RDDs
    *
    * @param e plan produced from BaseANF
    * @return string representing the Spark application of the input plan
    * using RDD as the base datatype.s
    */
  def generate(e: CExpr): String = e match {

    /** ZEROS **/
    case Null => "null"
    case CUnit => "()"
    case EmptySng => "Vector()"
    case EmptyCDict => s"()"
    case Index => "index"
    
    /** BASIC CONSTRUCTS **/
    case Variable(name, _) => name
    case InputRef(name, tp) => name
    case Constant(s:String) => "\"" + s + "\""
    case Constant(x) => x.toString
    case Sng(e) => s"Vector(${generate(e)})"
    case Label(fs) => {
      val tp = e.tp
      handleType(tp)
      val inner = fs.map{f => generate(f._2)}.mkString(", ")
      s"${generateType(tp)}($inner)"
    }
    case Record(fs) => {
      val tp = e.tp
      handleType(tp)
      s"${generateType(tp)}(${fs.map(f => generate(f._2)).mkString(", ")})"
    }
    case Tuple(fs) => s"(${fs.map(f => generate(f)).mkString(",")})"
    // this is a quick hack
    case Project(e1, "_LABEL") => generate(e1)
    case Project(e2 @ Record(fs), field) => 
      s"${generate(e2)}.${kvName(field)(fs.size)}"
    case Project(e2, field) => 
      s"${generate(e2)}.${kvName(field)}"
    /** MATH OPS **/
    case CMultiply(e1, e2) => s"${generate(e1)} * ${generate(e2)}"

    /** BOOL OPS **/
    case Equals(e1, e2) => s"${generate(e1)} == ${generate(e2)}"
    case Lt(e1, e2) => s"${generate(e1)} < ${generate(e2)}"
    case Gt(e1, e2) => s"${generate(e1)} > ${generate(e2)}"
    case Lte(e1, e2) => s"${generate(e1)} <= ${generate(e2)}"
    case Gte(e1, e2) => s"${generate(e1)} >= ${generate(e2)}"
    case And(e1, e2) => s"${generate(e1)} && ${generate(e2)}"
    case Or(e1, e2) => s"${generate(e1)} || ${generate(e2)}"
    case Not(e1) => s"!(${generate(e1)})"
    case If(cond, e1, Some(e2)) => conditional(cond, e1, e2)
    case If(cond, e1, None) => conditional(cond, e1, zero(e1))

    /** BAG UNION **/
    case Merge(e1, e2) => s"${generate(e1) ++ generate(e2)}"

    /** DEDUPLICATION **/
    case CDeDup(e1) => s"${generate(e1)}.distinct"

    case GroupDict(e1) => if (flatDict) generate(e1) else s"${generate(e1)}.groupBy(_++_)"

    case Bind(rv, CReduceBy(fd @ InputRef(_,_), v1 @ Variable(gv1, RecordCType(ms)), ks, vs), e2) =>
      val gfv = generate(fd)
      val gv1Tp = RecordCType(ms.filter(k => (ks.toSet ++ vs.toSet)(k._1)))
      handleType(gv1Tp)
      val finalValue = gv1Tp.attrTps.map(m => 
        if (vs.head == m._1) s"${gv1}._2" 
        else s"${gv1}._1.${m._1}").mkString(s"${generateType(gv1Tp)}(", ",", ")")
      val keys = Record(ks.map(k => k -> Project(v1, k)).toMap)
      handleType(keys.tp)
      val values = Project(v1, vs.head)
      val vzero = zero(values)
      // s"""|val $gfv = ${generate(fd)}
      s"""|val ${generate(rv)} = ${gfv}.mapPartitions(it => 
          | it.foldLeft(HashMap.empty[${generateType(keys.tp)}, ${generateType(values.tp)}]
          | .withDefaultValue($vzero))((acc, $gv1) => {
          |   acc(${generate(keys)}) += ${generate(values)}; acc
          |}).toVector.map($gv1 => $finalValue).iterator)
          |${generate(e2)}""".stripMargin

    case Bind(fv, fd @ FlatDict(i:InputRef), Bind(rv, CReduceBy(cr, v1, ks, vs), e2)) => 
      generate(Bind(rv, CReduceBy(i, v1, ks, vs), e2))

    case FlatDict(e1) => s"${generate(e1)} /** FLATTEN **/"

    /** DOMAIN CREATION **/
    case Reduce(e1, v, f @ Bind(_, Project(_, labelField), _), Constant(true)) if isDomain(e) =>  
      s"${generate(e1)}.createDomain(l => l.$labelField)"

    /** IDENTITY **/
    case Reduce(InputRef(n, _), v, Variable(_,_), Constant(true)) => n
    case Reduce(Variable(n, _), v, Variable(_,_), Constant(true)) => n

    /** PROJECT **/
    case Reduce(e1, v, f, Constant(true)) => 
      val vars = generateVars(v, e1.tp)
      s"""|${generate(e1)}.map{ case $vars => 
          |   {${generate(f)}}
          |}""".stripMargin

    case Reduce(e1, v, f, Constant("null")) => 
      val vars = generateVars(v, e1.tp)
      val gbl = if (hasLabel(f.tp)) ".group(_++_)" else ""
      s"""|${generate(e1)}.map{ case $vars => 
          |   ${nullProject(f)}
          |}$gbl""".stripMargin

    /** SELECT **/
    case Reduce(e1, v, f, p) => 
      val vars = generateVars(v, e1.tp)
      s"""|${generate(e1)}.map{ case $vars => 
          |   ${generate(f)}
          |}.filter($vars => {${generate(p)}})""".stripMargin
    // catch all
    case Select(x, v, p, e2) => generate(Reduce(x, List(v), e2, p))
    
    /** UNNEST **/
    case Unnest(e1, v1, f, v2, p, value) => 
      val vars = e1.tp match {
        case _:BagDictCType => generateVars(v1, e1.tp)+"._1"
        case _ => generateVars(v1, e1.tp)
      }
      val gv2 = generate(v2)
      val (v, attr, vs1, vs2, e2) = projectBag(f, v1, false)
      val nvars = generateVars(vs1, e1.tp)
      p match {
        case Constant(true) =>
          s"""|${generate(e1)}.flatMap{ case $vars => 
              | $v.$attr.map{ $gv2 => ($nvars, {${generate(value)}})}}""".stripMargin
        case _ => 
          s"""|${generate(e1)}.flatMap{ case $vars => 
              | $v.$attr.map{ $gv2 => ($nvars, {${generate(value)}})}
              |}.filter{ case ($vars, $gv2) => {${generate(p)}} }""".stripMargin
      }
    
    case OuterUnnest(e1, v1, f, v2, p, value) if e1.tp.isDict => 
      generate(Unnest(e1, v1, f, v2, p, value))
    case OuterUnnest(e1, v1, f, v2, p, value) => 
      val vars = generateVars(v1, e1.tp)
      val gv2 = generate(v2)
      val (v, attr, vs1, vs2, e2) = projectBag(f, v1, true)
      val gvalue = generate(value)
      
      val nvars = generateVars(vs1, e1.tp)
      val nullvars = if (v1.size == 1) "" 
        else s"case null => Vector((${generateVars(vs2, e1.tp)}, null))"
      
      val localAgg = e2 match {
        case Bind(lv1, CReduceBy(bv1, bv2, ks, vs), lv2) => 
          val lv = generate(bv2)
          val nvalue = value.tp match {
            case RecordCType(ms) => ms.map(m => 
              if (vs.head == m._1) s"${gv2}._2" 
              else s"${gv2}._1.${m._1}").mkString(s"${generateType(value.tp)}(", ",", ")")
            case _ => gvalue
          }
          val keys = Record(ks.map(k => k -> Project(bv2, k)).toMap)
          handleType(keys.tp)
          val values = Project(bv2, vs.head)
          val vzero = zero(values)
          s""".foldLeft(HashMap.empty[${generateType(keys.tp)}, ${generateType(values.tp)}].withDefaultValue($vzero))(
              (acc, $lv) => {
                acc(${generate(keys)}) += ${generate(values)}; acc
              }).toVector.map($gv2 => ($nvars, $nvalue))"""
        case _ => s".map($gv2 => ($nvars, {$gvalue}))"
      }

      p match {
        case Constant(true) =>
          s"""|${generate(e1)}.zipWithIndex.flatMap{ case ($vars, index) => 
              |  $v match { $nullvars
              |    case _ => if ($v.$attr.isEmpty) Vector(($nvars, null))
              |     else $v.$attr$localAgg
              |}}""".stripMargin
        case _ => 
          s"""|${generate(e1)}.flatMap{ case $vars => 
              | {${generate(f)}}.map{ $gv2 => ($nvars, {$gvalue})}
              |}.filter{ case ($vars, $gv2) => {${generate(p)}} }""".stripMargin
      }

    /** JOIN **/
    case Bind(jv, Join(e1, e2, v1, Constant(true), v2, Constant(true), proj1, proj2), e3) if isDomain(e1) => 
      s"""|val ${generate(jv)} = ${generate(e2)}.cartesianDomain(${generate(e1)})
          |${generate(e3)}""".stripMargin

    case Bind(jv, Join(e1, e2, v1, k1, v2, k2, proj1, proj2), e3) if isDomain(e1) => 
      val vars = generateVars(v1, e1.tp)
      val gv2 = generate(v2)
      val ve1 = "x" + Variable.newId()
      val ve2 = "x" + Variable.newId()
      val domain = e1.tp match {
        case BagCType(RecordCType(ms)) if ms.size == 1 => generate(e1)
        case _ => s"${generate(e1)}.map{ case $vars => ({${generate(k1)}}, $vars)}"
      }

      // cast a label to match a single label domain
      // needs to be tested for non-single label domains
      val tp = e1.tp.asInstanceOf[BagCType].tp.asInstanceOf[RecordCType].attrTps("_LABEL")
      //maybe the type has already been handled in domain above?
      handleType(tp)
      val label = generateType(tp)
      val e1key = k2 match {
        case Constant(true) => s"$label($gv2._1)"
        case _ => s"$label({${generate(k2)}})"
      }

      val nrec = drop(v2.tp, v2, "_1", false)

      val mapBagValues = e2.tp match {
        case BagCType(RecordCType(_)) => s"$gv2 => ($e1key, ${generate(nrec)})"
        case _ => s"(lbl, bag) => ($e1key, bag)"
      }

      s"""| val $ve1 = $domain
          | val $ve2 = ${generate(e2)}.map{ case $mapBagValues }
          | val ${generate(jv)} = $ve2.joinDomain($ve1)
          | ${generate(e3)}
        """.stripMargin

    case Bind(jv, Join(e1, e2, v1, p1, v2, p2, proj1, proj2), e3) => 
      val vars = generateVars(v1, e1.tp)
      val gv2 = generate(v2)
      val ve1 = "x" + Variable.newId()
      val ve2 = "x" + Variable.newId()
      (p1, p2) match {
        case (Constant(true), Constant(true)) =>
          s"val ${generate(jv)} = ${generate(e1)}.cartesian(${generate(e2)})\n${generate(e3)}"
        case _ => 
         s"""|val $ve1 = ${generate(e1)}.map{ case $vars => ({${generate(p1)}}, $vars) }
             |val $ve2 = ${generate(e2)}.map{ case $gv2 => ({${generate(p2)}}, $gv2) }
             |val ${generate(jv)} = $ve1.joinDropKey($ve2)
             |${generate(e3)}
             |""".stripMargin
      }

    /** LEFT OUTER JOIN **/
    case Bind(jv, OuterJoin(e1, e2, v1, p1, v2, p2, proj1, proj2), e3) if e1.tp.isDict =>
      generate(Bind(jv, Join(e1, e2, v1, p1, v2, p2, proj1, proj2), e3))
    case Bind(jv, OuterJoin(e1, e2, v1, p1, v2, p2, proj1, proj2), e3) => 
      val vars = generateVars(v1, e1.tp)
      val gv2 = generate(v2)
      val ve1 = "x" + Variable.newId()
      val ve2 = "x" + Variable.newId()
      // avoid null pointer exceptions
      val gp1 = p1 match {
        case Bind(_, proj @ Project(v, field), _) => 
          if (v1.size > 1) nullProject(p1, true)
          else generate(p1)
        case _ => sys.error(s"unsupported $p1")
      }
      s"""|val $ve1 = ${generate(e1)}.map{ case $vars => ({$gp1}, {${generate(proj1)}}) }
          |val $ve2 = ${generate(e2)}.map{ case $gv2 => ({${generate(p2)}}, {${generate(proj2)}}) }
          |val ${generate(jv)} = $ve1.leftOuterJoin($ve2).map{ case (k, (a, Some(v))) => 
          |    (a, v); case (k, (a, None)) => (a, null) }
          |${generate(e3)}
          |""".stripMargin

    /** NEST **/
    
    case Bind(v, CReduceBy(e1, v1 @ Variable(_, rt @ RecordCType(ms)), keys, values), e2) => 
      val gv1 = generate(v1)
      val keyTp = Record(keys.map(k => (k, Project(v1, k))).toMap)
      handleType(keyTp.tp)
      val skeys = keys.map(k => s"$gv1.$k"
        ).mkString(s"${generateType(keyTp.tp)}(", ",", ")")
      val svalue = s"${gv1}.${values.head}"
      val acc1 = "acc"+Variable.newId
      val acc = generate(v)
      val x = "x"+Variable.newId

      val nrec = v.tp match {
        case BagCType(TTupleType(fs)) => 
          val nrecTp = RecordCType((ms - "_1"))
          handleType(nrecTp)
          val xRange = if (fs.size <= 2) s"$x._1"
            else Range(1, fs.size).map(i => s"$x._1._$i").mkString(", ")
          (ms - "_1").keys.map(f => if (f == values.head) s"value" 
            else s"$x.$f").mkString(
              s"(${xRange}, ${generateType(nrecTp)}(", ", ", "))")
        case _ => 
          handleType(rt)
          ms.keys.map(f => 
            if (f == values.head) s"value" 
            else s"$x.$f").mkString(s"${generateType(rt)}(", ", ", ")")
      }

      s"""|val $acc1 = ${generate(e1)}.map($gv1 => 
          |   ($skeys, $svalue)).agg(_+_)
          |val $acc = $acc1.map{ case ($x, value) => $nrec }
          |${generate(e2)}""".stripMargin

    // TODO add filter
    case Nest(e1, v1, f, e2, v2, p, g, dk) =>

      // extract single element reduce
      val valueExp = e2 match {
        case Bind(_, proj:Project, Bind(_, Record(_), _)) => proj.tp match {
          case _:NumericType => proj
          case _ => e2
        }
        case _ => e2
      }
      val vars = generateVars(v1, e1.tp)
      val acc = "acc"+Variable.newId
      val emptyType = empty(valueExp)
      val gv2 = generate(v2)
      val baseKey = s"{${generate(f)}}"
      val key = f match {
        case Bind(bv, Project(v, field), _) => s"{${nullProject(f)}}"
        case _ => baseKey
      }
      val value = 
        if (!emptyType.contains("0")) s"Vector({${generate(valueExp)}})" 
        else s"{${generate(valueExp)}}"

      g match {
        case Bind(_, CUnit, _) =>
          s"""|${generate(e1)}.map{ 
              |  case $vars => ($baseKey, $value)
              |}.${agg(valueExp)}""".stripMargin
        case Bind(_, Tuple(fs), _) if fs.size > 1 => 
          ((s"${generate(e1)}.map{ case $vars => {${generate(g)}} match { " +:
            ((2 to fs.size).map(i => 
              if (i != fs.size) {
                s"case (${fs.slice(1, i).map(e => "_").mkString(",")},${zero(fs(i-1))},${fs.slice(i, fs.size).map(e => "_").mkString(",")}) => ($key, $emptyType)"
              } else { 
                s"case (${fs.slice(1, i).map(e => "_").mkString(",")},${zero(fs.last)}) => ($key, $emptyType)" //({${generate(f)}}, ${zero(e2)})" 
              }
            ) :+ s"case (null, ${(2 to fs.size).map(i => "_").mkString(",")}) => ($key, $emptyType)")
          ) :+ s"case $gv2 => ($baseKey, $value)\n}}.${agg(valueExp)}").mkString("\n")
        case _ => 
          s"""|${generate(e1)}.map{ case $vars => {${generate(g)}} match { 
              |  case (null) => ($key, $emptyType)
              |  case $gv2 => ($baseKey, $value)
              |}}.${agg(valueExp)}""".stripMargin
      }
    
    /** LOOKUPS **/

    // DOMAIN LOOKUP ITERATOR 
    case Bind(luv, Lookup(e1, e2, vs, k1, v2, k2, Constant(true)), e3) if isDomain(e1) =>
      val ve1 = "x" + Variable.newId()
      val ve2 = "x" + Variable.newId()
      val vars = generateVars(vs, e1.tp.asInstanceOf[BagCType].tp)
      val domain = e1.tp match {
        case BagCType(RecordCType(ms)) if ms.size == 1 => generate(e1)
        case _ => s"${generate(e1)}.map{ case $vars => ({${generate(k1)}}, $vars)}"
      }
      val tp = e1.tp.asInstanceOf[BagCType].tp.asInstanceOf[RecordCType].attrTps("_LABEL")
      handleType(tp)
      val label = generateType(tp)
      val e1key = k2 match {
        case Constant(true) => s"$label(lbl)"
        case _ => s"$label({${generate(k2)}})"
      }
      val gv2 = e2.tp match {
        case BagCType(RecordCType(_)) => s"${generate(v2)} => ($e1key, ${generate(v2)})"
        case _ => s"(lbl, bag) => ($e1key, bag)"
      }
      s"""| val $ve1 = $domain
          | val $ve2 = ${generate(e2)}.map{ case $gv2 }
          | val ${generate(luv)} = $ve2.lookupIteratorDomain($ve1)
          | ${generate(e3)}
        """.stripMargin

    // Non-domain lookup that requires flattening one of the dictionaires
    // ie. (parent dictionary).map(child label -> parent bag).lookup(child dictionary)
    // drops the partitioner of the parent dictionary, uses partitioning information 
    // of the child dictionary, then drops the child partitioner to rekey by 
    // the parent label.
    case Bind(unv, OuterUnnest(dict1, v1, bag, v2:Variable, filt, value), 
      Bind(luv, Lookup(_, dict2, _, Bind(_, Project(_, key1), _), v3, key2, key3), e2)) =>
      val vars = generateVars(v1, dict1.tp)
      val fdict = generate(unv)
      val gv2 = generate(v2)
      val gluv = generate(luv)
      val nv2 = generate(drop(v2.tp, v2, key1, false))
      s"""|val $fdict = ${generate(dict1)}.flatMap{
          | case $vars => {${generate(bag)}}.map{case $gv2 => 
          |   ($gv2.$key1, ($vars._1, $nv2))}
          |}
          |val $gluv = ${generate(dict2)}.rightCoGroupDropKey($fdict)
          |${generate(e2)}
          |""".stripMargin

    case Bind(fv, FlatDict(InputRef(pdict, _)), 
      Bind(_, _, Bind(lv, Lookup(e1, InputRef(cdict, _), vs, 
        p1 @ Bind(_, Project(v3:Variable, key1), _), v2, k2, p), e3))) =>

      val vars = generateVars(vs, e1.tp)
      val gv2 = generate(v2)
      val gluv = generate(lv)
      val ve1 = "x" + Variable.newId()
      val ve2 = "x" + Variable.newId()
      if (flatDict){
        val nv2 = generate(drop(v3.tp, v3, key1, false))
        val nv3 = generate(drop(v2.tp, v2, "_1", false))
        s"""|val $ve1 = $pdict.map{ case $vars => (${generate(v3)}.$key1, $nv2)}
            |val $ve2 = $cdict.map{ $gv2 => ($gv2._1, $nv3) }
            |val $gluv = $ve1.cogroupDropKey($ve2)
            |${generate(e3)}
        """.stripMargin
      }else "TODO"

    case Bind(rv, Reduce(InputRef(pdict, _), _,_,_), 
      Bind(lv, Lookup(e1, InputRef(cdict, _), vs, 
        p1 @ Bind(_, Project(v3:Variable, key1), _), v2, k2, p), e3)) =>

      val vars = generateVars(vs, e1.tp)
      val gv2 = generate(v2)
      val gluv = generate(lv)
      val ve1 = "x" + Variable.newId()
      val ve2 = "x" + Variable.newId()
      val nv2 = generate(drop(v3.tp, v3, key1, false))
      val nv3 = generate(drop(v2.tp, v2, "_1", false))
      if (flatDict){
        s"""|val $ve1 = $pdict.map{ case $vars => (${generate(v3)}.$key1, $nv2)}
            |val $ve2 = $cdict.map{ $gv2 => ($gv2._1, $nv3) }
            |val $gluv = $ve1.cogroupDropKey($ve2)
            |${generate(e3)}
        """.stripMargin
      }else "TODO"
      

    // Non-domain lookup that does not require flattening one of the dictionaries
    // ie. (top level bag).lookup(lower level dictionary)
    case Bind(luv, Lookup(e1, e2, v1, p1, v2, p2, v3), e3) =>
      val (v3, key1) = p1 match {
        case Bind(_, Project(pv:Variable, pk), _) => (pv, pk)
        case _ => ???
      }
      val vars = generateVars(v1, e1.tp.asInstanceOf[BagCType].tp)
      val gv2 = generate(v2)
      val gluv = generate(luv)
      val ve1 = "x" + Variable.newId()
      val ve2 = "x" + Variable.newId()
      // move this to the implementation of lookup
      val nrec = drop(v2.tp, v2, "_1")

      val cogroupFun =
        //if (e2.tp.isPartiallyShredded) s"val $gluv = $ve1.cogroupDropKey(${generate(e2)})"
        //else {
          e2.tp match {
            case BagCType(TTupleType(fs)) => (fs.head, fs.last) match {
              case (LabelType(_), BagCType(rs @ RecordCType(_))) => 
                handleType(rs)
                s"""|val $ve2 = ${generate(e2)}.map(v => 
                    |  (v._1, ${generateType(rs)}(${rs.attrTps.map(f => s"v.${f._1}").toList.mkString(", ")})))
                    |val $gluv = $ve1.cogroupDropKey($ve2)""".stripMargin
              case _ => s"val $gluv = $ve1.cogroupDropKey(${generate(e2)})"
            }
            case _ => s"val $gluv = $ve1.cogroupDropKey(${generate(e2)})"
          }
        // }
          // else s"${generate(e2)}.rightCoGroupDropKey($ve1)"
      val nv2 = generate(drop(v3.tp, v3, key1, false))
      s"""|val $ve1 = ${generate(e1)}.map{ case $vars => (${generate(v3)}.$key1, $nv2)}
          |$cogroupFun
          |${generate(e3)}
          |""".stripMargin 
    
    /** COMBINE JOIN / NEST OPERATORS **/

    // MERGE JOIN DOMAIN + GROUP BY KEY; LOOKUP DOMAIN
    case Bind(cv, CoGroup(e1, e2, vs, v2, k1, k2, value), e3) if isDomain(e1) =>
      val ve1 = "x" + Variable.newId()
      val ve2 = "x" + Variable.newId()
      val vars = generateVars(vs, e1.tp.asInstanceOf[BagCType].tp)
      val gv2 = generate(v2)

      val domain = e1.tp match {
        case BagCType(LabelType(ms)) if ms.size == 1 => generate(e1)
        case _ => s"${generate(e1)}.map{ case $vars => ({${generate(k1)}}, $vars)}"
      }

      // cast a label to match a single label domain
      // needs to be tested for non-single label domains
      val tp = e1.tp.asInstanceOf[BagCType].tp.asInstanceOf[RecordCType].attrTps("_LABEL")
      //maybe the type has already been handled in domain above?
      handleType(tp)
      val label = generateType(tp)
      val e1key = k2 match {
        case Constant(true) => s"$label($gv2._1)"
        case _ => s"$label({${generate(k2)}})"
      }

      val mapBagValues = e2.tp match {
        case BagCType(RecordCType(_)) => s"$gv2 => ($e1key, {${generate(value)}})"
        case _ => s"(lbl, bag) => ($e1key, bag.map(${generate(v2)} => {${generate(value)}}))"
      }
      s"""| val $ve1 = $domain
          | val $ve2 = ${generate(e2)}.map{ case $mapBagValues }
          | val ${generate(cv)} = $ve2.cogroupDomain($ve1)
          | ${generate(e3)}
        """.stripMargin

    // MERGE JOIN + GROUP BY
    case Bind(cv, CoGroup(e1, e2, vs, v2, k1, k2, value), e3) =>
      val ve1 = "x" + Variable.newId()
      val ve2 = "x" + Variable.newId()
      val vars = generateVars(vs, e1.tp)
      val key1 = if (vars.size > 1) nullProject(k1) else generate(k1)
      val gv2 = generate(v2)
      s"""| val $ve1 = ${generate(e1)}.map{ case $vars => ({$key1}, $vars)}
          | val $ve2 = ${generate(e2)}.map{ case $gv2 => ({${generate(k2)}}, {${generate(value)}})}
          | val ${generate(cv)} = $ve1.cogroupDropKey($ve2)
          | ${generate(e3)}
        """.stripMargin

    /** LOCAL COMPREHENSION **/

    case Comprehension(e1, v, p, e2) =>
      // val acc = Variable.fresh("acc")
      val cur = generate(v)
      p match {
        case Constant(true) => s"${generate(e1)}.map($cur => {${generate(e2)}} )"
        case _ => ???
      }

    /** SEQUENCE OF PLAN HANDLING **/

    case Bind(x, CNamed(n, e1), Bind(_, LinearCSet(_), _)) => 
      s"""|val $n = ${generate(e1)}
          |val ${generate(x)} = $n
          |${runJob(n, cache, evaluate)}""".stripMargin

    case Bind(x, CNamed(n, e1), e2) =>
      val (tmp1, tmp2) = if (cache) (false, false)
      else (cache, false)
    	s"""|val $n = ${generate(e1)}
          |val ${generate(x)} = $n
          |${runJob(n, tmp1, tmp2)}
          |${generate(e2)}""".stripMargin

    case Bind(v, LinearCSet(fs), e2) => 
      val gv = generate(v)
      s"""|val $gv = ${generate(fs.last)}
          |${runJob(gv, cache, evaluate)}""".stripMargin

    /** ANF BASE CASE **/
    case Bind(v, e1, e2) => 
      s"val ${generate(v)} = ${generate(e1)} \n${generate(e2)}"

    case _ => sys.error(s"not supported $e \n ${Printer.quote(e)}")
  }

  /** Generates a set of input vars based on a type 
    * ie. (a,b,c) => ((a,b),c)
    *
    * @param e list of variables from operator
    * @param tp type associated to the tupled variables
    * @return string tupled representation of variable list
    */
  private def generateVars(e: List[CExpr], tp: Type): String = tp match {
    case BagDictCType(BagCType(t), d) => generateVars(e, t)
    case BagCType(t) => generateVars(e, t)
    case TTupleType(seq) if (seq.size == 2 && validLabel(seq.head)) => s"${generate(e.head)}"
    case TTupleType(seq) if e.size == seq.size => e.map(generate).mkString("(", ", ", ")")
    case TTupleType(seq) if e.size > seq.size => {
      val en = e.dropRight(seq.size - 1)
      val rest = e.takeRight(seq.size - 1).map(generate).mkString(", ")
      s"(${generateVars(en, seq.head)}, $rest)"
    }
    case RecordCType(_) => s"${generate(e.head)}"
    case y if e.size == 1 => s"${generate(e.head)}" 
    case TTupleType(seq) => sys.error(s"not supported ${e.size} ${seq.size} --> $e:\n ")//${generateType(tp)}")
  }

  private def e1Key(p1: CExpr, p3: CExpr) =  p3 match {
    case Constant(true) => s"{${generate(p1)}}"
    case _ => s"({${generate(p1)}}, {${generate(p3)}})"
  }

  private def e2Key(v2: CExpr, p2: CExpr) = {
    val gv2 = generate(v2)
    p2 match {
      case Constant(true) => s".flatMapValues(identity)"
      case _ => ???
    }
  }


}