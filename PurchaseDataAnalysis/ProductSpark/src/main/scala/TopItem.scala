import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.Map
object TopItem {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("TopItem").setMaster("local")
    val sc = new SparkContext(conf)
    val usr_all= sc.textFile("hdfs://localhost:9000/user/hann/input/user_all").map(x => x.split(" "))
      .filter(x => x.length == 9).filter(x => x(5)=="1111")
    val usr_purchase = usr_all.filter(x => x(6)=="2").map(x => (x(1),1)).reduceByKey((x,y) => x+y)
    val usr_cart = usr_all.filter(x => x(6)=="1").map(x => ((x(0),x(1)),1)).reduceByKey((x,y) => x+y)
      .map(x=> (x._1._2,1)).reduceByKey((x,y) => x+y)
    val usr_col = usr_all.filter(x => x(6)=="3").map(x => ((x(0),x(1)),1)).reduceByKey((x,y) => x+y)
      .map(x=> (x._1._2,1)).reduceByKey((x,y) => x+y)
    val usr_2 = usr_purchase.union(usr_cart)
    val item_all = usr_2.union(usr_col).reduceByKey((x,y) => x+y).sortBy(_._2,false).take(100)
    item_all.foreach(println)
  }
}
