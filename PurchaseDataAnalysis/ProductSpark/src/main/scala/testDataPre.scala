import org.apache.spark.sql.SparkSession

object testDataPre {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().master("local").appName("DataPrepare").getOrCreate()
    val df1 = spark.read.csv("hdfs://localhost:9000/user/hann/user_log")
    val df2 = spark.read.csv("hdfs://localhost:9000/user/hann/user_info")
    val df3 = spark.read.csv("hdfs://localhost:9000/user/hann/test")
    val col1 = Seq("user_id", "item_id", "cat_id", "merchant_id", "brand_id", "time_stamp", "action_type")
    val col2 = Seq("user_id", "age_range", "gender")
    val col3 = Seq("user_id", "merchant_id", "label")
    val df_log = df1.toDF(col1 : _*)
    val df_info = df2.toDF(col2 : _*)
    val df_test = df3.toDF(col3: _*)
    val joinE2 = add_mean.col("user_id") === df_info.col("user_id")
    val test_info = add_mean.join(df_info,joinE2, "left_outer").drop(df_info("user_id"))

    //count missing values
    //val columns=test_info.columns
    //val missing_cnt=columns.map(x=>test_info.select(col(x)).where(col(x).isNull).count)
    //missing_cnt.foreach(println)
  }
}
