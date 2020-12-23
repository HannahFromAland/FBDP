import org.apache.spark.sql.SparkSession

object CustomerGender {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().master("local").appName("BrandViewTop10").getOrCreate()
    import spark.implicits._
    val df1= spark.read.csv("hdfs://localhost:9000/user/hann/user_log")
    val df2= spark.read.csv("hdfs://localhost:9000/user/hann/user_info")
    val col1 = Seq("user_id","item_id","cat_id","merchant_id","brand_id","time_stamp","action_type")
    val col2 = Seq("user_id","age_range","gender")
    val df_log = df1.toDF(col1 : _*)
    val df_info = df2.toDF(col2 : _*)
    val joinExpression = df_log.col("user_id") === df_info.col("user_id")
    val df_all = df_log.join(df_info, joinExpression, "left_outer").where("time_stamp = '1111' and action_type = '2' and gender in (0,1)" )
    df_all.groupBy("gender").count().show()
  }
}
