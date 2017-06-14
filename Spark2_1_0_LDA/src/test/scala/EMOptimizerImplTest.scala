import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

/**
  * Created by Ricardo Barona on 6/13/17.
  *
  * Tests EMOptimizerImpl.runEMTest method.
  * Actual test includes what is printed and the number of topics returned.
  *
  * Output example:
  * ________________________________
  * |                               |
  * |3 topics:                      |
  * |TOPIC 0                        |
  * |data    0.01156017429813764    |
  * |World   0.009880989449857738   |
  * |series  0.0095383325012586     |
  * |Zidane  0.007946951767899644   |
  * |Messi   0.0072244277615203575  |
  * |game    0.0065658545168441156  |
  * |Call    0.006502050613431901   |
  * |video   0.006497440693673674   |
  * |first   0.0062117329310362     |
  * |FIFA    0.005949652939759374   |
  * |_______________________________|
  **/
class EMOptimizerImplTest extends FlatSpec with BeforeAndAfter with Matchers {

  val sparkSession = SparkSession.builder
    .appName("Spark 2.1.0 LDA Unit Tests")
    .master("local")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  before {
    System.out.println(sparkSession.sparkContext.appName)
  }

  after {
    sparkSession.stop
  }

  "runEMTest" should "return 3 topics with 10 top terms each topic" in {

    val fileLocation = "data/wikipedia.json"
    val numTopics = 3
    val numIterations = 20

    val json = sparkSession.read.json(fileLocation)

    val topics = EMOptimizerImpl.runEMTest(sparkSession, json, numTopics, numIterations)

    // Testing three topics are returned and each topic contains the top 10 terms.
    topics.length shouldBe 3
    topics(0).length shouldBe 10
    topics(1).length shouldBe 10
    topics(2).length shouldBe 10
  }

}
