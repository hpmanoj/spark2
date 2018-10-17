package com.kpn.udex.spark.kafkastreamlearining
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
// Import SQL functions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.sql.{Row, SQLContext}

case class LabeledDocument(id: Long, text: String, label: Double)

case class Document(id: Long, text: String)

object firstMachineLearining {
	def main(args: Array[String]) {

		val sparkConf = new SparkConf().setMaster("local[2]").setAppName("firstMachineLearining")
				val sparkContext = new SparkContext(sparkConf)
				val sqlContext = new SQLContext(sparkContext)
				import sqlContext._
				import sqlContext.implicits._
				// Prepare training documents, which are labeled.
				val training = sparkContext.parallelize(Seq(
						LabeledDocument(0L, "a b c d e spark", 1.0),
						LabeledDocument(1L, "b d", 0.0),
						LabeledDocument(2L, "spark f g h", 1.0),
						LabeledDocument(3L, "hadoop mapreduce", 0.0),
						LabeledDocument(4L, "b spark who", 1.0),
						LabeledDocument(5L, "g d a y", 0.0),
						LabeledDocument(6L, "spark fly", 1.0),
						LabeledDocument(7L, "was mapreduce", 0.0),
						LabeledDocument(8L, "e spark program", 1.0),
						LabeledDocument(9L, "a e c l", 0.0),
						LabeledDocument(10L, "spark compile", 1.0),
						LabeledDocument(11L, "hadoop software", 0.0)))

				// Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
				val tokenizer = new Tokenizer()
				.setInputCol("text")
				.setOutputCol("words")
				val hashingTF = new HashingTF()
				.setInputCol(tokenizer.getOutputCol)
				.setOutputCol("features")
				val lr = new LogisticRegression()
				.setMaxIter(10)
				val pipeline = new Pipeline()
				.setStages(Array(tokenizer, hashingTF, lr))

				// We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
				// This will allow us to jointly choose parameters for all Pipeline stages.
				// A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
				val crossval = new CrossValidator()
				.setEstimator(pipeline)
				.setEvaluator(new BinaryClassificationEvaluator)
				// We use a ParamGridBuilder to construct a grid of parameters to search over.
				// With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
				// this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
				val paramGrid = new ParamGridBuilder()
				.addGrid(hashingTF.numFeatures, Array(10, 100, 1000))
				.addGrid(lr.regParam, Array(0.1, 0.01))
				.build()
				crossval.setEstimatorParamMaps(paramGrid)
				crossval.setNumFolds(2) // Use 3+ in practice

				// Run cross-validation, and choose the best set of parameters.
				val cvModel = crossval.fit(training.toDF())
				// Get the best LogisticRegression model (with the best set of parameters from paramGrid).
				val lrModel = cvModel.bestModel

				// Prepare test documents, which are unlabeled.
				val test = sparkContext.parallelize(Seq(
						Document(4L, "spark i j k"),
						Document(5L, "l m n"),
						Document(6L, "mapreduce spark"),
						Document(7L, "apache hadoop")))

				// Make predictions on test documents. cvModel uses the best model found (lrModel).
				cvModel.transform(test.toDF())
				.select('id, 'text, 'prediction)
				.collect()
				.foreach { case Row(id: Long, text: String, prediction: Double) =>
				println("(" + id + ", " + text + ") " + ", prediction=" + prediction)
		}
	}

}
