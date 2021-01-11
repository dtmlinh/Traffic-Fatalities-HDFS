Name: Linh Dinh

spark-shell --conf spark.hadoop.metastore.catalog.default=hive



import org.apache.spark.sql.SaveMode
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.feature.{StringIndexer, IndexToString, VectorIndexer}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors



val data = spark.table("ldinh_fars_crss").withColumnRenamed("FATAL_FLAG","label")

val assembler = new VectorAssembler()
  .setInputCols(Array("VE_TOTAL","VE_FORMS","PVH_INVL","PEDS","PERMVIT","PERNOTMVIT","MONTH","YEAR","DAY_WEEK","HOUR","HARM_EV","MAN_COLL","RELJCT1","RELJCT2","TYP_INT", "WRK_ZONE","REL_ROAD","LGT_COND","WEATHER","SCH_BUS","CF1","CF2","CF3"))
  .setOutputCol("features")

val output = assembler.transform(data)
output.select("features", "label").show()

val labelIndexer = new StringIndexer()
  .setInputCol("label")
  .setOutputCol("indexedLabel")

val featureIndexer = new VectorIndexer()
  .setInputCol("features")
  .setOutputCol("indexedFeatures")
  .setMaxCategories(20)
  .setHandleInvalid("keep")

val Array(trainingData, testData) = output.select("features", "label").randomSplit(Array(0.7, 0.3))

val rf = new RandomForestClassifier()
  .setLabelCol("indexedLabel")
  .setFeaturesCol("indexedFeatures")
  .setNumTrees(100)
  .setMinInstancesPerNode(5)
  .setMaxDepth(20)
  .setFeatureSubsetStrategy("sqrt")

val pipeline = new Pipeline()
  .setStages(Array(labelIndexer, featureIndexer, rf))

val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("indexedLabel")
  .setPredictionCol("prediction")
  .setMetricName("accuracy")

val pipelineModel = pipeline.fit(trainingData)

val pipelineTrainPrediction = pipelineModel.transform(trainingData)
//pipelineTrainPrediction.show(10)
val pipelineTestPrediction = pipelineModel.transform(testData)
//pipelineTestPrediction.show(10)

val tr_accuracy = evaluator.evaluate(pipelineTrainPrediction)
println("Train Error = " + (1.0 - tr_accuracy))
val te_accuracy = evaluator.evaluate(pipelineTestPrediction)
println("Test Error = " + (1.0 - te_accuracy))

val rfModel = pipelineModel.stages(2).asInstanceOf[RandomForestClassificationModel]
println("Learned classification forest model:\n" + rfModel.toDebugString)

val importances = pipelineModel.stages.last.asInstanceOf[RandomForestClassificationModel].featureImportances

importances.toArray.zipWithIndex
  .map(_.swap)
  .sortBy(-_._2)
  .foreach(x => println(x._1 + " -> " + x._2))



///RandomForestClassifier Feature Importances Results:

//11 -> 0.1875558074197628 "MAN_COLL"
//10 -> 0.1158303841988735 "HARM_EV"
//13 -> 0.09256857503836294 "RELJCT2"
//14 -> 0.0896243686308224 "TYP_INT"
//9 -> 0.06335124411295627 "HOUR" --> to show in app
//18 -> 0.06075909492402046 "WEATHER" --> to show in app
//6 -> 0.055021975056411605 "MONTH" --> to show in app
//16 -> 0.054144414828903385 "REL_ROAD"
//17 -> 0.04900789301308252 "LGT_COND" --> to show in app
//0 -> 0.043937152698774504 "VE_TOTAL"
//1 -> 0.03466315297142903 "VE_FORMS"
//8 -> 0.03304960945814706 "DAY_WEEK" --> to show in app
//4 -> 0.027159115426062926 "PERMVIT"
//20 -> 0.02004421955554573 "CF1"
//3 -> 0.01825133825626911 "PEDS
//5 -> 0.017628153341381646 "PERNOTMVIT"
//7 -> 0.0141063328447094 "YEAR"
//12 -> 0.008372299193299697 "RELJCT1" --> to show in app
//2 -> 0.007840047453479618 "PVH_INVL
//21 -> 0.002849048977847526 "CF2"
//15 -> 0.002657610263405279 "WRK_ZONE"
//22 -> 0.0011888211387845685 "CF3"
//19 -> 3.8934119766807833E-4 "SCH_BUS"



//0 "VE_TOTAL", 
//1 "VE_FORMS", 
//2 "PVH_INVL",
//3 "PEDS",
//4 "PERMVIT",
//5 "PERNOTMVIT",
//6 "MONTH", 
//7 "YEAR",
//8 "DAY_WEEK", 
//9 "HOUR", 
//10 "HARM_EV", 
//11 "MAN_COLL",
//12 "RELJCT1", 
//13 "RELJCT2", 
//14 "TYP_INT",
//15 "WRK_ZONE",
//16 "REL_ROAD",
//17 "LGT_COND", 
//18 "WEATHER", 
//19 "SCH_BUS",
//20 "CF1",
//21 "CF2",
//22 "CF3"


