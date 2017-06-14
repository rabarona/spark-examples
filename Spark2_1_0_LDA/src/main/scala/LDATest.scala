import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by Ricardo Barona on 6/13/17.
  *
  * Prerequisites: Upload wikipedia.json to HDFS folder with
  * > hadoop fs -copyFromLocal wikipedia.json <HDFS_FOLDER>/
  *
  * Execution in cluster:
  * spark2-submit --master yarn --driver-memory 4g --class "LDATest" \
  * spark2_1_0_lda_2.11-1.0.jar <HDFS_FOLDER>/wikipedia.json 3 100
  *
  * Dependencies: Spark 2.1.0
  *
  */
object LDATest {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkSession = SparkSession.builder
      .appName("Spark 2.1.0 LDA test")
      .getOrCreate()

    val fileLocation = args(0)
    val numTopics = args(1).toInt
    val numIterations = args(2).toInt

    val json = sparkSession.read.json(fileLocation)

    EMOptimizerImpl.runEMTest(sparkSession, json, numTopics, numIterations)

    sparkSession.stop

    System.exit(0)
  }

}

case class WikiExtract(extract: Array[String], title: String)
