import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.mllib.clustering.{DistributedLDAModel, EMLDAOptimizer, LDA}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable

/**
  * Created by Ricardo Barona on 6/13/17.
  *
  * Spark 2.1.0 mllib LDA implementation test using EM optimizer
  *
  */
object EMOptimizerImpl {

  /**
    * Runs LDA using EM optimizer and converting distributed model to online model.
    *
    * @param sparkSession  a previously created spark session.
    * @param json          a data frame with wikipedia information (see data/wikipedia.json)
    * @param numTopics     the number of topics to discover
    * @param numIterations the number of iterations for LDA
    */
  def runEMTest(sparkSession: SparkSession, json: DataFrame, numTopics: Int, numIterations: Int):
  Array[Array[(String, Double)]] = {

    import sparkSession.implicits._

    val totalDocuments = json.count
    val data = json
      .map({
        case Row(extract: String, title: String) => WikiExtract(extract.split("\\s"), title)
      })
      .toDF

    val stopWordRemover = new StopWordsRemover()
      .setInputCol("extract")
      .setOutputCol("filtered_extract")

    val filtered = stopWordRemover.transform(data)
    val corpus: RDD[Array[String]] = filtered
      .select("filtered_extract")
      .rdd
      .map({
        case Row(extract: Seq[String]) => extract
          .toArray
      })

    val termCounts: Array[(String, Long)] = corpus
      .flatMap(_.map(_ -> 1L))
      .reduceByKey(_ + _)
      .collect
      .sortBy(-_._2)

    val vocabArray: Array[String] = termCounts
      .takeRight(termCounts.length)
      .map(_._1)

    val vocab: Map[String, Int] = vocabArray
      .zipWithIndex
      .toMap

    val documents: RDD[(Long, Vector)] = corpus.zipWithIndex
      .map {
        case (tokens, id) => val counts = new mutable.HashMap[Int, Double]()
          tokens.foreach {
            term =>
              if (vocab.contains(term)) {
                val idx = vocab(term)
                counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
              }
          }
          (id, Vectors.sparse(vocab.size, counts.toSeq))
      }

    val optimizer = new EMLDAOptimizer
    val ldaModelEMOptimizer: DistributedLDAModel = new LDA()
      .setK(numTopics)
      .setMaxIterations(numIterations)
      .setAlpha(1.02)
      .setBeta(1.001)
      .setOptimizer(optimizer)
      .run(documents)
      .asInstanceOf[DistributedLDAModel]

    val localLDAModel: DistributedLDAModel = ldaModelEMOptimizer

    val docTopicDistEMOptimizer: RDD[(Long, Vector)] = localLDAModel.topicDistributions

    docTopicDistEMOptimizer
      .take(totalDocuments.toInt)
      .foreach(println)

    val topicIndices = ldaModelEMOptimizer.describeTopics(maxTermsPerTopic = 10)

    val topics = topicIndices
      .map {
        case (terms, termWeights) =>
          terms
            .zip(termWeights)
            .map {
              case (term, weight) => (vocabArray(term.toInt), weight)
            }
      }

    println(s"Total $numTopics topics:")
    topics
      .zipWithIndex
      .foreach {
        case (topic, i) => println(s"TOPIC $i")
          topic.foreach { case (term, weight) =>
            println(s"$term\t$weight")
          }
          println()
      }

    topics
  }

}
