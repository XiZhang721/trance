package shredding.generator

import shredding.core._
import shredding.wmcc._
import shredding.examples.genomic._
import shredding.examples.simple._
import shredding.examples.tpch._

/**
  * Generates Scala code for a provided query
  */

object App {
 
  def main(args: Array[String]){
    // exp1.0
    // exp1.1
    // runExperiment1FN()
    // exp2.0
    // exp2.1
    runExperiment1NN()
    // exp3.0
    // runExperiment2FN()
    // runExperiment2NN()
    // // exp2.0
    // runExperiment2()
    // runDataset()
  }

  def runDataset(){
    val pathout = "spark/src/main/scala/sprkloader/experiments"
    // Utils.flatOpt(Query1BU, pathout, "Flat++,Standard,Query1")
    // Utils.shredDataset(Query1Full, pathout, "Shred,Standard,Query1")
    // Utils.shredDataset(Query1Full, pathout, "Shred,Standard,Query1")
    // Utils.runDataset(Query1BU, pathout, "Flat++,Standard,Query1")
    // Utils.runDataset(Query1BU, pathout, "Flat++,Standard,Query1", skew = true)
    // Utils.shredDataset(Query1Full, pathout, "Shred,Standard,Query1")
    // Utils.shredDataset(Query1Full, pathout, "Shred,Standard,Query1", unshred = true)
    // Utils.shredDataset(Query1Full, pathout, "Shred,Skew,Query1", skew = true)
    // Utils.shredDataset(Query1Full, pathout, "Shred,Skew,Query1", skew = true, unshred = true)
    // Utils.shredDataset(Test2Full, pathout, "Shred,Standard,Test2Full")
    // Utils.shredDataset(Test2Full, pathout, "Shred,Standard,Test2Full", unshred = true)
    // Utils.runDataset(Test2FullFlat, pathout, "Flat++,2")
    Utils.runDataset(Test1, pathout, "Flat++,1")
    Utils.runDataset(Test2Flat, pathout, "Flat++,1")
    Utils.runDatasetInput(Test1, Test1NN, pathout, "Flat++,1")
    Utils.runDatasetInput(Test2Flat, Test2NN, pathout, "Flat++,2")
    Utils.runDatasetInput(Test3FullFlat, Test3NN, pathout, "Flat++,3")
    Utils.runDatasetInput(Test4FullFlat, Test4NN, pathout, "Flat++,4")
  }

  def runExperiment1FN(){
    // val pathout = "experiments/exp1.1/"
    val pathout = "spark/src/main/scala/sprkloader/experiments"

    // Utils.flatDataset(Test0, pathout, "Flat,0", opt = 0)
    // Utils.flatDataset(Test0Full, pathout, "Flat,0", opt = 0)
    // Utils.flatDataset(Test1, pathout, "Flat,1", opt = 0)
    // Utils.flatDataset(Test1Full, pathout, "Flat,1", opt = 0)
    // Utils.flatDataset(Test2, pathout, "Flat,2", opt = 0)
    // Utils.flatDataset(Test2Full, pathout, "Flat,2", opt = 0)
    // Utils.flatDataset(Test3, pathout, "Flat,3", opt = 0)
    // Utils.flatDataset(Test3Full, pathout, "Flat,3", opt = 0)
    // Utils.flatDataset(Test4, pathout, "Flat,4", opt = 0)
    // Utils.flatDataset(Test4Full, pathout, "Flat,4", opt = 0)

    // Utils.flatDataset(Test0, pathout, "Flat+,0", opt = 1)
    // Utils.flatDataset(Test0Full, pathout, "Flat+,0", opt = 1)
    // Utils.flatDataset(Test1, pathout, "Flat+,1", opt = 1)
    // Utils.flatDataset(Test1Full, pathout, "Flat+,1", opt = 1)
    // Utils.flatDataset(Test2, pathout, "Flat+,2", opt = 1)
    // Utils.flatDataset(Test2Full, pathout, "Flat+,2", opt = 1)
    // Utils.flatDataset(Test3, pathout, "Flat+,3", opt = 1)
    // Utils.flatDataset(Test3Full, pathout, "Flat+,3", opt = 1)
    // Utils.flatDataset(Test4, pathout, "Flat+,4", opt = 1)
    // Utils.flatDataset(Test4Full, pathout, "Flat+,4", opt = 1)

    Utils.flatDataset(Test0, pathout, "Flat++,0")
    Utils.flatDataset(Test0Full, pathout, "Flat++,0")
    Utils.flatDataset(Test1, pathout, "Flat++,1")
    Utils.flatDataset(Test1Full, pathout, "Flat++,1")
    Utils.flatDataset(Test2Flat, pathout, "Flat++,2")
    Utils.flatDataset(Test2FullFlat, pathout, "Flat++,2")
    Utils.flatDataset(Test3Flat, pathout, "Flat++,3")
    Utils.flatDataset(Test3FullFlat, pathout, "Flat++,3")
    Utils.flatDataset(Test4Flat, pathout, "Flat++,4")
    Utils.flatDataset(Test4FullFlat, pathout, "Flat++,4")

    Utils.shredDataset(Test0, pathout, "Shred,0")
    Utils.shredDataset(Test1, pathout, "Shred,1")
    Utils.shredDataset(Test2, pathout, "Shred,2")
    Utils.shredDataset(Test3, pathout, "Shred,3")
    Utils.shredDataset(Test4, pathout, "Shred,4")

    Utils.shredDataset(Test0Full, pathout, "Shred,0")
    Utils.shredDataset(Test1Full, pathout, "Shred,1")
    Utils.shredDataset(Test2Full, pathout, "Shred,2")
    Utils.shredDataset(Test3Full, pathout, "Shred,3")
    Utils.shredDataset(Test4Full, pathout, "Shred,4")

    Utils.shredDataset(Test0, pathout, "Shred,0", unshred=true)
    Utils.shredDataset(Test1, pathout, "Shred,1", unshred=true)
    Utils.shredDataset(Test2, pathout, "Shred,2", unshred=true)
    Utils.shredDataset(Test3, pathout, "Shred,3", unshred=true)
    Utils.shredDataset(Test4, pathout, "Shred,4", unshred=true)

    Utils.shredDataset(Test0Full, pathout, "Shred,0", unshred=true)
    Utils.shredDataset(Test1Full, pathout, "Shred,1", unshred=true)
    Utils.shredDataset(Test2Full, pathout, "Shred,2", unshred=true)
    Utils.shredDataset(Test3Full, pathout, "Shred,3", unshred=true)
    Utils.shredDataset(Test4Full, pathout, "Shred,4", unshred=true)
  }
 
  def runExperiment1NN(){
    val pathout = "spark/src/main/scala/sprkloader/experiments"

    // Utils.flatInput(Test0Full, Test0FullNN, pathout, "Flat,0")
    // Utils.flatInput(Test1Full, Test1FullNN, pathout, "Flat,1")
    // Utils.flatInput(Test2FullFlat, Test2FullNN, pathout, "Flat,2")
    // Utils.flatInput(Test3FullFlat, Test3FullNN, pathout, "Flat,3")
    // Utils.flatInput(Test4FullFlat, Test4FullNN, pathout, "Flat,4")

    // Utils.flatInput(Test0Full, Test0NN, pathout, "Flat,0")
    // Utils.flatInput(Test1Full, Test1NN, pathout, "Flat,1")
    // Utils.flatInput(Test2FullFlat, Test2NN, pathout, "Flat,2")
    // Utils.flatInput(Test3FullFlat, Test3NN, pathout, "Flat,3")
    // Utils.flatInput(Test4FullFlat, Test4NN, pathout, "Flat,4")

    // Utils.flatProjInput(Test0Full, Test0FullNN, pathout, "Flat+,0")
    // Utils.flatProjInput(Test1Full, Test1FullNN, pathout, "Flat+,1")
    // Utils.flatProjInput(Test2FullFlat, Test2FullNN, pathout, "Flat+,2")
    // Utils.flatProjInput(Test3FullFlat, Test3FullNN, pathout, "Flat+,3")
    // Utils.flatProjInput(Test4FullFlat, Test4FullNN, pathout, "Flat+,4")

    // Utils.flatProjInput(Test0Full, Test0NN, pathout, "Flat+,0")
    // Utils.flatProjInput(Test1Full, Test1NN, pathout, "Flat+,1")
    // Utils.flatProjInput(Test2FullFlat, Test2NN, pathout, "Flat+,2")
    // Utils.flatProjInput(Test3FullFlat, Test3NN, pathout, "Flat+,3")
    // Utils.flatProjInput(Test4FullFlat, Test4NN, pathout, "Flat+,4")

    Utils.runDatasetInput(Test0Full, Test0NN, pathout, "Flat++,0")
    Utils.runDatasetInput(Test1Full, Test1NN, pathout, "Flat++,1")
    Utils.runDatasetInput(Test2FullFlat, Test2NN, pathout, "Flat++,2")
    Utils.runDatasetInput(Test3FullFlat, Test3NN, pathout, "Flat++,3")
    Utils.runDatasetInput(Test4FullFlat, Test4NN, pathout, "Flat++,4")

    Utils.runDatasetInput(Test0Full, Test0FullNN, pathout, "Flat++,0")
    Utils.runDatasetInput(Test1Full, Test1FullNN, pathout, "Flat++,1")
    Utils.runDatasetInput(Test2FullFlat, Test2FullNN, pathout, "Flat++,2")
    Utils.runDatasetInput(Test3FullFlat, Test3FullNN, pathout, "Flat++,3")
    Utils.runDatasetInput(Test4FullFlat, Test4FullNN, pathout, "Flat++,4")

    Utils.runDatasetInputShred(Test0Full, Test0NN, pathout, "Shred,0")
    Utils.runDatasetInputShred(Test1Full, Test1NN, pathout, "Shred,1")
    Utils.runDatasetInputShred(Test2Full, Test2NN, pathout, "Shred,2")
    Utils.runDatasetInputShred(Test3Full, Test3NN, pathout, "Shred,3")
    Utils.runDatasetInputShred(Test4Full, Test4NN, pathout, "Shred,4")

    Utils.runDatasetInputShred(Test0Full, Test0NN, pathout, "Shred,0", unshred=true)
    Utils.runDatasetInputShred(Test1Full, Test1NN, pathout, "Shred,1", unshred=true)
    Utils.runDatasetInputShred(Test2Full, Test2NN, pathout, "Shred,2", unshred=true)
    Utils.runDatasetInputShred(Test3Full, Test3NN, pathout, "Shred,3", unshred=true)
    Utils.runDatasetInputShred(Test4Full, Test4NN, pathout, "Shred,4", unshred=true)

    Utils.runDatasetInputShred(Test0Full, Test0FullNN, pathout, "Shred,0")
    Utils.runDatasetInputShred(Test1Full, Test1FullNN, pathout, "Shred,1")
    Utils.runDatasetInputShred(Test2Full, Test2FullNN, pathout, "Shred,2")
    Utils.runDatasetInputShred(Test3Full, Test3FullNN, pathout, "Shred,3")
    Utils.runDatasetInputShred(Test4Full, Test4FullNN, pathout, "Shred,4")

    Utils.runDatasetInputShred(Test0Full, Test0FullNN, pathout, "Shred,0", unshred=true)
    Utils.runDatasetInputShred(Test1Full, Test1FullNN, pathout, "Shred,1", unshred=true)
    Utils.runDatasetInputShred(Test2Full, Test2FullNN, pathout, "Shred,2", unshred=true)
    Utils.runDatasetInputShred(Test3Full, Test3FullNN, pathout, "Shred,3", unshred=true)
    Utils.runDatasetInputShred(Test4Full, Test4FullNN, pathout, "Shred,4", unshred=true)
  }

  def runExperiment2FN(){

    val pathout = "spark/src/main/scala/sprkloader/experiments"

    Utils.flatDataset(Test0, pathout, "Flat++,0", skew = true)
    Utils.flatDataset(Test0Full, pathout, "Flat++,0", skew = true)
    Utils.flatDataset(Test1, pathout, "Flat++,1", skew = true)
    Utils.flatDataset(Test1Full, pathout, "Flat++,1", skew = true)
    Utils.flatDataset(Test2Flat, pathout, "Flat++,2", skew = true)
    Utils.flatDataset(Test2FullFlat, pathout, "Flat++,2", skew = true)
    Utils.flatDataset(Test3Flat, pathout, "Flat++,3", skew = true)
    Utils.flatDataset(Test3FullFlat, pathout, "Flat++,3", skew = true)
    Utils.flatDataset(Test4Flat, pathout, "Flat++,4", skew = true)
    Utils.flatDataset(Test4FullFlat, pathout, "Flat++,4", skew = true)

    Utils.shredDataset(Test0, pathout, "Shred,0", skew = true)
    Utils.shredDataset(Test1, pathout, "Shred,1", skew = true)
    Utils.shredDataset(Test2, pathout, "Shred,2", skew = true)
    Utils.shredDataset(Test3, pathout, "Shred,3", skew = true)
    Utils.shredDataset(Test4, pathout, "Shred,4", skew = true)

    Utils.shredDataset(Test0Full, pathout, "Shred,0", skew = true)
    Utils.shredDataset(Test1Full, pathout, "Shred,1", skew = true)
    Utils.shredDataset(Test2Full, pathout, "Shred,2", skew = true)
    Utils.shredDataset(Test3Full, pathout, "Shred,3", skew = true)
    Utils.shredDataset(Test4Full, pathout, "Shred,4", skew = true)

    Utils.shredDataset(Test0, pathout, "Shred,0", unshred=true, skew = true)
    Utils.shredDataset(Test1, pathout, "Shred,1", unshred=true, skew = true)
    Utils.shredDataset(Test2, pathout, "Shred,2", unshred=true, skew = true)
    Utils.shredDataset(Test3, pathout, "Shred,3", unshred=true, skew = true)
    Utils.shredDataset(Test4, pathout, "Shred,4", unshred=true, skew = true)

    Utils.shredDataset(Test0Full, pathout, "Shred,0", unshred=true, skew = true)
    Utils.shredDataset(Test1Full, pathout, "Shred,1", unshred=true, skew = true)
    Utils.shredDataset(Test2Full, pathout, "Shred,2", unshred=true, skew = true)
    Utils.shredDataset(Test3Full, pathout, "Shred,3", unshred=true, skew = true)
    Utils.shredDataset(Test4Full, pathout, "Shred,4", unshred=true, skew = true)

  }

  def runExperiment2NN(){
    val pathout = "spark/src/main/scala/sprkloader/experiments"

    // Utils.runDatasetInput(Test0Full, Test0NN, pathout, "Flat++,0", skew = true)
    // Utils.runDatasetInput(Test1Full, Test1NN, pathout, "Flat++,1", skew = true)
    Utils.runDatasetInput(Test2FullFlat, Test2NN, pathout, "Flat++,2", skew = true)
    // Utils.runDatasetInput(Test3FullFlat, Test3NN, pathout, "Flat++,3", skew = true)
    // Utils.runDatasetInput(Test4FullFlat, Test4NN, pathout, "Flat++,4", skew = true)

    // Utils.runDatasetInput(Test0Full, Test0FullNN, pathout, "Flat++,0", skew = true)
    // Utils.runDatasetInput(Test1Full, Test1FullNN, pathout, "Flat++,1", skew = true)
    // Utils.runDatasetInput(Test2FullFlat, Test2FullNN, pathout, "Flat++,2", skew = true)
    // Utils.runDatasetInput(Test3FullFlat, Test3FullNN, pathout, "Flat++,3", skew = true)
    // Utils.runDatasetInput(Test4FullFlat, Test4FullNN, pathout, "Flat++,4", skew = true)

    // Utils.runDatasetInputShred(Test0Full, Test0NN, pathout, "Shred,0", skew = true)
    // Utils.runDatasetInputShred(Test1Full, Test1NN, pathout, "Shred,1", skew = true)
    Utils.runDatasetInputShred(Test2Full, Test2NN, pathout, "Shred,2", skew = true)
    // Utils.runDatasetInputShred(Test3Full, Test3NN, pathout, "Shred,3", skew = true)
    // Utils.runDatasetInputShred(Test4Full, Test4NN, pathout, "Shred,4", skew = true)

    // Utils.runDatasetInputShred(Test0Full, Test0NN, pathout, "Shred,0", unshred=true, skew = true)
    // Utils.runDatasetInputShred(Test1Full, Test1NN, pathout, "Shred,1", unshred=true, skew = true)
    // Utils.runDatasetInputShred(Test2Full, Test2NN, pathout, "Shred,2", unshred=true, skew = true)
    // Utils.runDatasetInputShred(Test3Full, Test3NN, pathout, "Shred,3", unshred=true, skew = true)
    // Utils.runDatasetInputShred(Test4Full, Test4NN, pathout, "Shred,4", unshred=true, skew = true)

    // Utils.runDatasetInputShred(Test0Full, Test0FullNN, pathout, "Shred,0", skew = true)
    // Utils.runDatasetInputShred(Test1Full, Test1FullNN, pathout, "Shred,1", skew = true)
    // Utils.runDatasetInputShred(Test2Full, Test2FullNN, pathout, "Shred,2", skew = true)
    // Utils.runDatasetInputShred(Test3Full, Test3FullNN, pathout, "Shred,3", skew = true)
    // Utils.runDatasetInputShred(Test4Full, Test4FullNN, pathout, "Shred,4", skew = true)

    // Utils.runDatasetInputShred(Test0Full, Test0FullNN, pathout, "Shred,0", unshred=true, skew = true)
    // Utils.runDatasetInputShred(Test1Full, Test1FullNN, pathout, "Shred,1", unshred=true, skew = true)
    // Utils.runDatasetInputShred(Test2Full, Test2FullNN, pathout, "Shred,2", unshred=true, skew = true)
    // Utils.runDatasetInputShred(Test3Full, Test3FullNN, pathout, "Shred,3", unshred=true, skew = true)
    // Utils.runDatasetInputShred(Test4Full, Test4FullNN, pathout, "Shred,4", unshred=true, skew = true)
  }

  def runExperiment2(){
    val pathout = "experiments/exp2.0"
    // Utils.flatOpt(Query1BU, pathout, "Flat++,Standard,Query1")
    // Utils.flatOpt(Query1BU, pathout, "Flat++,Skew,Query1", skew = true)
    // Utils.shred(Query1, pathout, "Shred,Standard,Query1")
    // Utils.shred(Query1, pathout, "Shred,Standard,Query1", unshred=true)
    // Utils.shred(Query1, pathout, "Shred,Skew,Query1", skew = true)
    // Utils.shred(Query1, pathout, "Shred,Skew,Query1", skew = true, unshred=true)

    // Utils.flatOptInput(Test2Flat, Query4, pathout, "Flat++,Standard,Query4")
    // Utils.flatOptInput(Test2Flat, Query4, pathout, "Flat++,Skew,Query4", skew = true)
    // Utils.shredInput(Test2, Query4, pathout, "Shred,Standard,Query4")
    // Utils.shredInput(Test2, Query4, pathout, "Shred,Standard,Query4", unshred=true)
    // Utils.shredInput(Test2, Query4, pathout, "Shred,Skew,Query4", skew = true)
    // Utils.shredInput(Test2, Query4, pathout, "Shred,Skew,Query4", skew = true, unshred=true)

    Utils.flatOpt(Query5, pathout, "Flat++,Standard,Query5")
    Utils.flatOpt(Query5, pathout, "Flat++,Skew,Query5", skew = true)
    Utils.shred(Query5, pathout, "Shred,Standard,Query5")
    Utils.shred(Query5, pathout, "Shred,Standard,Query5", unshred=true)
    Utils.shred(Query5, pathout, "Shred,Skew,Query5", skew = true)
    Utils.shred(Query5, pathout, "Shred,Skew,Query5", skew = true, unshred=true)

    Utils.flatOptInput(Query5, Query6, pathout, "Flat++,Standard,Query6")
    Utils.flatOptInput(Query5, Query6, pathout, "Flat++,Skew,Query6", skew = true)
    Utils.shredInput(Query5, Query6Full, pathout, "Shred,Standard,Query6")
    Utils.shredInput(Query5, Query6Full, pathout, "Shred,Standard,Query6", unshred=true)
    Utils.shredInput(Query5, Query6Full, pathout, "Shred,Skew,Query6", skew = true)
    Utils.shredInput(Query5, Query6Full, pathout, "Shred,Skew,Query6", skew = true, unshred=true)


  }

  def runExperiment1Joins(){
    val pathout = "experiments/exp1.3"

    // Utils.flat(Test0Join, pathout, "Flat,0")
    // Utils.flat(Test1Join, pathout, "Flat,1")
    // Utils.flat(Test2Join, pathout, "Flat,2")
    // Utils.flat(Test3Join, pathout, "Flat,3")
    // Utils.flat(Test4Join, pathout, "Flat,4")

    // Utils.flatProj(Test0Join, pathout, "Flat+,0")
    // Utils.flatProj(Test1Join, pathout, "Flat+,1")
    // Utils.flatProj(Test2Join, pathout, "Flat+,2")
    // Utils.flatProj(Test3Join, pathout, "Flat+,3")
    // Utils.flatProj(Test4Join, pathout, "Flat+,4")

    // Utils.flatOpt(Test0Join, pathout, "Flat++,0")
    // Utils.flatOpt(Test1Join, pathout, "Flat++,1")
    // Utils.flatOpt(Test2JoinFlat, pathout, "Flat++,2")
    // Utils.flatOpt(Test3JoinFlat, pathout, "Flat++,3")
    // Utils.flatOpt(Test4JoinFlat, pathout, "Flat++,4")

    // Utils.shredDomains(Test0Join, pathout, "ShredDom,0")
    // Utils.shredDomains(Test1Join, pathout, "ShredDom,1")
    // Utils.shredDomains(Test2Join, pathout, "ShredDom,2")
    // Utils.shredDomains(Test3Join, pathout, "ShredDom,3")
    // Utils.shredDomains(Test4Join, pathout, "ShredDom,4")

    // Utils.unshredDomains(Test0Join, pathout, "ShredDom,0")
    // Utils.unshredDomains(Test1Join, pathout, "ShredDom,1")
    // Utils.unshredDomains(Test2Join, pathout, "ShredDom,2")
    // Utils.unshredDomains(Test3Join, pathout, "ShredDom,3")
    // Utils.unshredDomains(Test4Join, pathout, "ShredDom,4")
  }
  
  def runTPCH1(){   

    // Flattning, no shredding
    //Utils.runSparkNoDomains(TPCHQuery1Full, false, true)
    // Utils.runSparkNoDomains(Test2a, false, false)

    // Run shred without domains, cannot do unshredding
    // this has a conflict with some changes in the unnesting algorithm
    // that needs fixed
    // Utils.runSparkInputNoDomains(TPCHQuery1Full, TPCHQuery4FullAgg)

    // Run shred with domains
    // the true adds unshredding
    //Utils.runSparkDomains(TPCHQuery1Full, true, true)
    // Utils.runSparkDomains(Test2, true, false)

    // Run shred with domains when a query is used as input for another
    // the true adds unshredding
    //Utils.runSparkInputDomains(TPCHQuery1Full, TPCHQuery4FullAgg, false, true)
    // Utils.runSparkInputDomains(TPCHQuery1Full, TPCHQuery4FullAgg, false, false)
  }

  /**
    Examples that generate queries that generate a non-spark scala program
  **/

  val runner = new PipelineRunner{}
  val translator = new NRCTranslator{}
  val normalizer = new Finalizer(new BaseNormalizer{})
  val tpchInputM = TPCHSchema.tpchInputs.map(f => translator.translate(f._1) -> f._2)
  val tpchShredM = tpchInputM ++ TPCHSchema.tpchShredInputs
 

  def run1Calc(){
    
    println("---------------------------- Query 1  ----------------------------")  
    val q1 = translator.translate(TPCHQueries.query1.asInstanceOf[translator.Expr])
    val qinfo = (q1.asInstanceOf[CExpr], TPCHQueries.q1name, TPCHQueries.q1data)
    Utils.runCalc(qinfo, tpchInputM)

    println("---------------------------- Query 1 Shred ----------------------------")  
    val sq1 = runner.shredPipeline(TPCHQueries.query1.asInstanceOf[runner.Expr])
    val sqinfo = (sq1.asInstanceOf[CExpr], "Shred"+TPCHQueries.q1name, TPCHQueries.sq1data)
    Utils.runCalc(sqinfo, tpchShredM)

  }
 
  def run1(){
    
    println("---------------------------- Query 1 Unnest ----------------------------")  
    val q1 = translator.translate(TPCHQueries.query1.asInstanceOf[translator.Expr])
    val qinfo = (q1.asInstanceOf[CExpr], TPCHQueries.q1name, TPCHQueries.q1data)
    Utils.run(qinfo, tpchInputM)

    println("---------------------------- Query 1 Shred Unnest ----------------------------")
    val sq1 = runner.shredPipeline(TPCHQueries.query1.asInstanceOf[runner.Expr])
    val sqinfo = (sq1.asInstanceOf[CExpr], "Shred"+TPCHQueries.q1name, TPCHQueries.sq1data)
    Utils.run(sqinfo, tpchShredM)

  }


}
