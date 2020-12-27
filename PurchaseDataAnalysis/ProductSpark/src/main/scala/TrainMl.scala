import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{Vector,Vectors}
import org.apache.spark.ml.{Pipeline,PipelineModel}
import org.apache.spark.ml.feature.{StringIndexer, VectorIndexer}
import org.apache.spark.ml.classification.{LogisticRegressionModel, LogisticRegression
  ,RandomForestClassifier,RandomForestClassificationModel}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

object TrainMl {
  case class Customer(features: Vector, label: Double)
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().appName("PredictReturnedCustomer").master("local").getOrCreate()
    import spark.implicits._

    val train= spark.sparkContext.textFile("file:///home/hann/Desktop/feature_total/").map(x => x.split(",")).filter(x => x.length == 12).map(p => Customer(Vectors.dense(p(3).toDouble, p(4).toDouble,p(5).toDouble,p(6).toDouble,p(7).toDouble,p(8).toDouble,p(9).toDouble,p(10).toDouble,p(11).toDouble), p(2).toDouble)).toDF()
    val Array(trainData, testData) = train.randomSplit(Array(0.7,0.3))
    val testDataNum:Double = testData.count()
    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(train)
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").fit(train)
    //-------------------------------------------------LogisticRegression-------------------------------------------------
    println("------------------------LogisticRegression---------------------------")
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    val lrModel = lr.fit(trainData)
    val Predictions = lrModel.transform(testData)

    val evaluatorM = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction")
    val Accuracy = evaluatorM.setMetricName("accuracy").evaluate(Predictions)
    println("\nAccuracy: " +Accuracy)
    val Precision = evaluatorM.setMetricName("weightedPrecision").evaluate(Predictions)
    println("\nweightedPrecision: " +Precision)
    val Recall = evaluatorM.setMetricName("weightedRecall").evaluate(Predictions)
    println("\nweightedRecall: " +Recall)

    val evaluatorB = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("rawPrediction")
    val AUC = evaluatorB.setMetricName("areaUnderROC").evaluate(Predictions)
    println("\nareaUnderROC: " +AUC)
    val PR = evaluatorB.setMetricName("areaUnderPR").evaluate(Predictions)
    println("\nareaUnderPR: " +PR)

    //-------------------------------------------------CV: LogisticRegression-------------------------------------------------
    println("------------------------CV: LogisticRegression---------------------------")
    val lr = new LogisticRegression().setMaxIter(10).setElasticNetParam(0.8)
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, lr))
    // use a ParamGridBuilder to construct a grid of parameters to search over.
    // With 2 values for lr.regParam,
    // this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .build()

    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    // This will allow us to jointly choose parameters for all Pipeline stages.
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    // Note that the evaluator here is a BinaryClassificationEvaluator and its default metric
    // is areaUnderROC.
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)

    val cvModel = cv.fit(trainData)
    val Predictions = cvModel.transform(test)

    val evaluatorM = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction")
    val Accuracy = evaluatorM.setMetricName("accuracy").evaluate(Predictions)
    println("\nAccuracy: " +Accuracy)
    val Precision = evaluatorM.setMetricName("weightedPrecision").evaluate(Predictions)
    println("weightedPrecision: " +Precision)
    val Recall = evaluatorM.setMetricName("weightedRecall").evaluate(Predictions)
    println("weightedRecall: " +Recall)

    val evaluatorB = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("rawPrediction")
    val AUC = evaluatorB.setMetricName("areaUnderROC").evaluate(Predictions)
    println("areaUnderROC: " +AUC)
    val PR = evaluatorB.setMetricName("areaUnderPR").evaluate(Predictions)
    println("areaUnderPR: " +PR)
    // view best parameters
    val bestModel= cvModel.bestModel.asInstanceOf[PipelineModel]
    val lrModel=bestModel.stages(2).asInstanceOf[LogisticRegressionModel]
    println("BestRegParam:" +lrModel.getRegParam)

    //-------------------------------------------------RandomForest-------------------------------------------------
    println("------------------------RandomForest---------------------------")
    val rf = new RandomForestClassifier().setNumTrees(20)

    val rfModel = rf.fit(trainData)
    val Predictions = rfModel.transform(testData)

    val evaluatorM = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction")
    val Accuracy = evaluatorM.setMetricName("accuracy").evaluate(Predictions)
    println("\nAccuracy: " +Accuracy)
    val Precision = evaluatorM.setMetricName("weightedPrecision").evaluate(Predictions)
    println("weightedPrecision: " +Precision)
    val Recall = evaluatorM.setMetricName("weightedRecall").evaluate(Predictions)
    println("weightedRecall: " +Recall)

    val evaluatorB = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("rawPrediction")
    val AUC = evaluatorB.setMetricName("areaUnderROC").evaluate(Predictions)
    println("areaUnderROC: " +AUC)
    val PR = evaluatorB.setMetricName("areaUnderPR").evaluate(Predictions)
    println("areaUnderPR: " +PR)
    //-------------------------------------------------CV: RandomForest-------------------------------------------------
    println("------------------------CV: RandomForest---------------------------")
    val rf=new RandomForestClassifier()
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, rf))
    val paramGrid = new ParamGridBuilder().addGrid(rf.maxDepth, Array(5,10)).addGrid(rf.numTrees, Array(10,20)) .build()

    val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(new BinaryClassificationEvaluator).setEstimatorParamMaps(paramGrid).setNumFolds(5)

    val cvModel = cv.fit(trainData)
    val Predictions = cvModel.transform(testData)

    val evaluatorM = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction")
    val Accuracy = evaluatorM.setMetricName("accuracy").evaluate(Predictions)
    println("\nAccuracy: " +Accuracy)
    val Precision = evaluatorM.setMetricName("weightedPrecision").evaluate(Predictions)
    println("weightedPrecision: " +Precision)
    val Recall = evaluatorM.setMetricName("weightedRecall").evaluate(Predictions)
    println("weightedRecall: " +Recall)

    val evaluatorB = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("rawPrediction")
    val AUC = evaluatorB.setMetricName("areaUnderROC").evaluate(Predictions)
    println("areaUnderROC: " +AUC)
    val PR = evaluatorB.setMetricName("areaUnderPR").evaluate(Predictions)
    println("areaUnderPR: " +PR)
    // view best parameters
    val bestModel= cvModel.bestModel.asInstanceOf[PipelineModel]
    val rfModel=bestModel.stages(2).asInstanceOf[RandomForestClassificationModel]
    println("BestMaxDepth:" +rfModel.getMaxDepth)
    println("BestNumTrees:" +rfModel.getNumTrees)

  }
}
