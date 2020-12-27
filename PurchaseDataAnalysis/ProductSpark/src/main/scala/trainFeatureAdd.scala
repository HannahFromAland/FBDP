import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object trainFeatureAdd {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().master("local").appName("DataPrepare").getOrCreate()
    val df1 = spark.read.csv("hdfs://localhost:9000/user/hann/user_log/")
    val df2 = spark.read.csv("hdfs://localhost:9000/user/hann/train_info")
    val col1 = Seq("user_id", "item_id", "cat_id", "merchant_id", "brand_id", "time_stamp", "action_type")
    val col2 = Seq("user_id", "merchant_id", "label", "age_range", "gender")
    val df_log = df1.toDF(col1: _*)
    val df_title = df2.toDF(col2: _*).select("user_id", "merchant_id")
    /**
     * purchase
     */
    val re_pur = df_log.where("action_type=2").groupBy("user_id", "merchant_id").count()
    val title_add1 = df_title.join(re_pur, df_title.col("user_id") === re_pur.col("user_id") and
      df_title.col("merchant_id") === re_pur.col("merchant_id"), "left_outer").select(df_title("user_id"), df_title("merchant_id"), re_pur("count").as("pur"))
    title_add1.cache()

    /** add cart
     */
    val add_cart = df_log.where("action_type=2").groupBy("user_id", "merchant_id").count()
    val tmp1 = title_add1.join(add_cart, title_add1.col("user_id") === add_cart.col("user_id") and
      title_add1.col("merchant_id") === add_cart.col("merchant_id"), "left_outer").select(title_add1("*"), add_cart("count").as("add_cart"))

    val cart_all = df_log.where("action_type=2").groupBy("user_id").count()
    val tmp2 = tmp1.join(cart_all, tmp1.col("user_id") === cart_all.col("user_id"), "left_outer").select(tmp1("*"), cart_all("count").as("cart_all"))
    tmp2.cache()
    val title_add2 = tmp2.withColumn("per_cart", expr("add_cart/cart_all")).select("user_id", "merchant_id", "re_pur", "per_cart")

    /**
     * add collection
     */
    val add_collect = df_log.where("action_type=3").groupBy("user_id", "merchant_id").count()
    val tmp3 = title_add2.join(add_collect, title_add2.col("user_id") === add_collect.col("user_id") and
      title_add2.col("merchant_id") === add_collect.col("merchant_id"), "left_outer").select(title_add2("*"), add_collect("count").as("add_collect"))

    val collect_all = df_log.where("action_type=3").groupBy("user_id").count()
    val tmp4 = tmp3.join(collect_all, tmp3.col("user_id") === collect_all.col("user_id"), "left_outer").select(tmp3("*"), collect_all("count").as("collect_all"))
    val tmp5 = tmp4.na.fill(0)
    val title_add3_final = tmp5.withColumn("per_collect", expr("add_collect/collect_all")).select("user_id", "merchant_id", "re_pur", "per_cart")

    /**
     * add click
     */
    val click = df_log.where("action_type=0").groupBy("user_id", "merchant_id").count()
    val c1 = title_add3_final.join(click, title_add3_final.col("user_id") === click.col("user_id") and
      title_add3_final.col("merchant_id") === click.col("merchant_id"), "left_outer").select(title_add3_final("*"), click("count").as("click"))
    val c2 = c1.withColumn("click_fill", when($"click".isNull, c1.select(mean("click"))
      .first()(0).asInstanceOf[Double])
      .otherwise($"click"))
    val c4 = c2.select("user_id", "merchant_id", "pur", "per_cart", "per_collect", "click_fill")

    val click_all = df_log.where("action_type=0").groupBy("user_id").count()
    val c5 = c1.join(click_all, c1.col("user_id") === click_all.col("user_id"), "left_outer").select(c1("*"), click_all("count").as("click_all"))
    val c6 = c5.withColumn("click_all_f", when($"click_all".isNull, c5.select(mean("click_all")).first()(0).asInstanceOf[Double]).otherwise($"click_all"))
    val title_add4 = c6.select("user_id", "merchant_id", "pur", "per_cart", "per_collect", "click_fill", "click_all_f")
    val revise = c8.withColumn("per_click", when($"per_click".isNull, c8.select(mean("per_click"))
      .first()(0).asInstanceOf[Double])
      .otherwise($"per_click"))

    /**
     * merchant feature1: category of merchant
     */

    val cat = df_log.select("merchant_id", "cat_id").distinct().groupBy("merchant_id").count()
    val add_cat = df_title.join(cat, df_title.col("merchant_id") === cat.col("merchant_id"), "left_outer").select(df_title("*"), cat("count").as("mer_cat"))

    /**
     * merchant feature2: category brought by user
     */

    val buy_cat = df_log.select("user_id", "merchant_id", "cat_id").distinct().groupBy("user_id", "merchant_id").count()
    val add_buy_cat = add_cat.join(buy_cat, add_cat.col("user_id") === buy_cat.col("user_id") and add_cat.col("merchant_id") === buy_cat.col("merchant_id"), "left_outer").select(add_cat("*"), buy_cat("count").as("buy_cat"))

    /**
     * merchant feature3: direct competitors
     */
    val player = df_log.select("cat_id", "brand_id", "merchant_id").distinct().groupBy("cat_id", "brand_id").count()
    val comp = df_log.select("merchant_id", "brand_id", "cat_id").distinct()
    val comp1 = comp.join(player, comp.col("brand_id") === player.col("brand_id") and comp.col("cat_id") === player.col("cat_id"), "left_outer").select(comp("*"), player("count").as("competitors"))
    df.select($"_1".alias("x1"))
    val add_com = add_buy_cat.join(rename, add_buy_cat.col("merchant_id") === rename.col("merchant_id"), "left_outer").select(add_buy_cat("*"), rename("sum_com"))

    /**
     * average info of merchant
     * */
    val inf = spark.read.csv("hdfs://localhost:9000/user/hann/user_info")
    val colinf = Seq("user_id", "age_range", "gender")
    val df_info = inf.toDF(colinf: _*)
    val mer_info = df_log.where("action_type=2").select("user_id", "merchant_id").distinct()
    val add_user_info = mer_info.join(df_info, mer_info.col("user_id") === df_info.col("user_id"), "left_outer").select(mer_info("*"), df_info("age_range"), df_info("gender"))
    val mean_mer = res51.groupBy("merchant_id").agg(avg("age_range").as("mean_age"), avg("gender").as("mean_gender")).show()
    val add_mean = add_com.join(mean_mer, add_com.col("merchant_id") === mean_mer.col("merchant_id"), "left_outer").select(add_com("*"), mean_mer("mean_age", "mean_gender"))
    val final = df_all.join(res63, df_all.col("user_id") === res63.col("user_id") and df_all.col("merchant_id") === res63.col("merchant_id"), "left_outer").drop(res63("user_id"), res63("merchant_id"))
    val final_train = df_label.join(df_feature, df_label.col("user_id") === df_feature.col("user_id") and df_label.col("merchant_id") === df_feature.col("merchant_id")).drop(df_feature("user_id"), df_feature("merchant_id")) select(df_label("*"), df_feature("pur"), df_feature("per_cart"), df_feature("per_collect"), df_feature("per_click"), df_feature("mer_cat"), df_feature("buy_cat"), df_feature("sum_com"), df_feature("mean_age"), df_feature("mean_gender"))


    /**
     * final merge
     */
    val df2 = spark.read.csv("hdfs://localhost:9000/user/hann/train_info")
    val df1 = spark.read.csv("file:///home/hann/Desktop/feature_all")
    val col2 = Seq("user_id", "merchant_id", "label", "age_range", "gender")
    val col1 = Seq("user_id", "merchant_id", "pur", "per_cart", "per_collect", "per_click", "mer_cat", "buy_cat", "sun_com", "mean_age", "mean_gender")
    val df_label = df2.toDF(col2: _*)
    val df_feature = df1.toDF(col1: _*)
    val final_train = df_label.join(df_feature, df_label.col("user_id") === df_feature.col("user_id") and df_label.col("merchant_id") === df_feature.col("merchant_id"))
      .select(df_label("*"), df_feature(""))
  }
}
