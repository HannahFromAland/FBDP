
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object finalFeature {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().master("local").appName("DataPrepare").getOrCreate()
    val df2 = spark.read.csv("hdfs://localhost:9000/user/hann/train_info")
    val df1 = spark.read.csv("file:///home/hann/Desktop/feature_all")
    val col2 = Seq("user_id", "merchant_id", "label", "age_range", "gender")
  val col1 = Seq("user_id", "merchant_id", "pur", "per_cart", "per_collect", "per_click", "mer_cat", "buy_cat", "sum_com", "mean_age", "mean_gender")
    val df_label = df2.toDF(col2: _*)
    df_label.cache()
    val df_feature = df1.toDF(col1: _*)
    df_feature.cache()
    val final_train = df_label.join(df_feature, df_label.col("user_id") === df_feature.col("user_id") and df_label.col("merchant_id") === df_feature.col("merchant_id"))
      .select(df_label("*"), df_feature("pur"),df_feature("per_cart"),df_feature("per_collect"),df_feature("per_click")
      ,df_feature("mer_cat"),df_feature("buy_cat"),df_feature("sum_com"),df_feature("mean_age"),df_feature("mean_gender"))
    val add_age = final_train.withColumn("age_dis",expr("age_range - mean_age"))
    val add_gender = add_age.withColumn("gender_dis",expr("gender - mean_gender"))
    val feature = add_gender.drop("age_range","gender","mean_age","mean_gender").filter("age_dis is not null and gender_dis is not null").repartition(1)

    feature.write.csv("file:///home/hann/Desktop/feature_total")
  }
}
