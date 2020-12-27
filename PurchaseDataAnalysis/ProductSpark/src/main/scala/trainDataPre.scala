import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, when}

object trainDataPre {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().master("local").appName("DataPrepare").getOrCreate()
    val df2= spark.read.csv("hdfs://localhost:9000/user/hann/user_info")
    val df4= spark.read.csv("hdfs://localhost:9000/user/hann/train")
    val col2 = Seq("user_id","age_range","gender")
    val col3 = Seq("user_id","merchant_id","label")
    val df_info = df2.toDF(col2 : _*)
    val df_train = df4.toDF(col3: _*)
  val col = Seq("user_id","merchant_id","pur","per_cart","per_collect","per_click","mer_cat","buy_cat","sum_com","mean_age","mean_gender")
    // filter the label and drop null value
    val train = df_train.filter("label in (0,1)").na.drop().repartition(1)

    //merge the information of user with predict datasets
    val joinE1 = df_train.col("user_id") === df_info.col("user_id")

    val train_info = train.join(df_info,joinE1, "left_outer").drop(df_info("user_id"))
    train_info.write.csv("hdfs://localhost:9000/user/hann/train_info")
    //missing process
    //val train_info_has_age = train_info.withColumn("has_age", expr("case when age_range in (1,2,3,4,5,6,7,8) then 1 " +
      //"else 0 end"))
    //val train_info_final = train_info_has_age.withColumn("has_gender", expr("case when gender in (0,1) then 1 " +
      //"else 0 end"))
    //val train_info_final = train_info.where("gender in (0,1) and age_range in (1,2,3,4,5,6,7,8)")


  }
}
