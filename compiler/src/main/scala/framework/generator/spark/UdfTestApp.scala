package framework.generator.spark

import framework.examples._
import framework.examples.genomic._
import framework.examples.translation._

object UdfTestApp extends App {

  override def main(args: Array[String]){
    val testQuery = Query11
    val queryIndex = 11
    // runs the standard pipeline
    val standard_label = "ExperimentTest, Query" + queryIndex.toString + ",standard"
    val shredded_label = "ExperimentTest, Query" + queryIndex.toString + ",shredded"
    try{
      AppWriter.runDataset(testQuery, "ExperimentTest,standard", optLevel = 1)
    }catch {
      case e:Exception => println("Generation for " + standard_label + " failed")
    }
    try{
      AppWriter.runDatasetShred(testQuery, "ExperimentTest,shredded", optLevel = 1)
    }catch {
      case e:Exception =>println("Generation for " + shredded_label + " failed")
    }

    	// notebk = true, zhost = "oda-compute-0-6",
     //  zport = 8085)

    // runs the shredded pipeline


  }
}