# 实验四 

## 实验环境准备

### 版本说明

- Ubuntu 16.04
- JDK 1.8
- Hadoop 3.3.0
- Hive  3.1.2
- Scala 2.11.6
- Spark 3.0.1

## 最热门商品和最受年轻人关注商家

### MapReduce实现

- 首先需要通过MapReduce实现用户行为日志和用户画像的`join`操作 [ReduceJoin](https://github.com/HannahFromAland/FBDP/blob/main/PurchaseDataAnalysis/Product/src/main/java/join/ReduceJoin.java)

- `join`操作可以在Map端实现，也可以在Reduce端实现；Map端适合两表中有一个较小表，可以先将其读入缓存，再和另一张大表进行匹配,Reduce端操作集中于Reducer，容易造成数据倾斜（但数据集里`user_info`已经4.5MB了...于是选择Reduce端操作，并将有缺失值的数据作删除处理)
- 处理后的文件预览

![image.png](https://i.loli.net/2020/12/15/ZSfQ8eYzcv1wH3D.png)

- 将join后的总表重命名为`user_all`之后上传至HDFS

```bash
$ mv part-r-00000 user_all
$ hdfs dfs -ls  /user/hann/input
```

- 统计最受欢迎的商品（最受欢迎商家的三类数据处理标准类似）

> 根据**最受欢迎**的定义：添加购物车+购买+添加收藏夹前100名
>
> 需要考虑的去重问题：
>
> - 由于购买应该是商品受欢迎程度最重要的决定因素，同一用户多次重复购买特定商品也是表明商品受欢迎程度的影响因素而非噪音，因此对购买操作仅进行计数处理（即不需要对同一用户多次操作进行去重）
> - 而多次添加收藏夹的操作对用户和对应商品吸引力之间的业务意义判定较弱，且可能存在负面效果（某种程度上代表购买欲望不强因此出现多次删除又加入收藏夹的情况），因此决定对添加收藏夹的用户操作进行去重处理
> - 多次添加购物车则存在更多种可能的情形和解读方式（比如有用户单纯把购物车和收藏夹当做同一种用途，而另一种则是每次购买都会先添加购物车再结算），但若通过ROI进行判断，多次添加购物车和收藏夹的操作都不会给商家带来更多回报，因此最终选择进行去重处理
> - 假设同一用户的购买和添加购物车/收藏夹操作之间不产生重复影响
> - 进一步优化方向（但因为感觉略不符合题意所以没实现hh只是对于该分析逻辑的想法）：对三种行为的计数进行加权，如`单次购买行为：单次添加购物车：单次添加收藏夹=50%：30%：20%`，可以有效区分被过多添加收藏夹/购物车的商品与购买次数多的商品之间的“受欢迎程度”

- 使用两次MapReduce实现：第一次MapReduce实现去重和频率统计，第二次MapReduce进行频率倒排（源文件分别为[AllItem](https://github.com/HannahFromAland/FBDP/blob/main/PurchaseDataAnalysis/Product/src/main/java/PopularItem/AllItem.java)以及[PopMerYoung](https://github.com/HannahFromAland/FBDP/blob/main/PurchaseDataAnalysis/Product/src/main/java/PopularItem/PopMerYoung.java)
- 收藏+加入购物车对用户及对应商品和店铺利用`HashSet`实现去重操作
- 结果见[Product/PopularItem](https://github.com/HannahFromAland/FBDP/blob/main/PurchaseDataAnalysis/Product/PopularItem) 及[Product/PopularMerchant](https://github.com/HannahFromAland/FBDP/blob/main/PurchaseDataAnalysis/Product/PopularMerchant)

### Spark实现

- 首先配置Spark环境（为了调试和交互方便选择`Spark+IntelliJ+Maven配置`）

  - 在启动界面的Configure - Plugins中（或在已有的项目界面中的File-Settings-Plugins），找到Scala，点击安装
  - 新建一个Maven项目，project SDK选择java 1.8
  - 在Project Structure - Ploatform Settings - Global Libraries中，添加scala SDK
  - 添加好后右键点击添加的SDK，点击`Copy to Project Libraries`并apply
  - `pom.xml`配置如下

  ```xml
  <properties>
          <spark.version>2.1.0</spark.version>
          <scala.version>2.11</scala.version>
      </properties>
  
  
      <dependencies>
          <dependency>
              <groupId>org.apache.spark</groupId>
              <artifactId>spark-core_${scala.version}</artifactId>
              <version>${spark.version}</version>
          </dependency>
          <dependency>
              <groupId>org.apache.spark</groupId>
              <artifactId>spark-streaming_${scala.version}</artifactId>
              <version>${spark.version}</version>
          </dependency>
          <dependency>
              <groupId>org.apache.spark</groupId>
              <artifactId>spark-sql_${scala.version}</artifactId>
              <version>${spark.version}</version>
          </dependency>
          <dependency>
              <groupId>org.apache.spark</groupId>
              <artifactId>spark-hive_${scala.version}</artifactId>
              <version>${spark.version}</version>
          </dependency>
          <dependency>
              <groupId>org.apache.spark</groupId>
              <artifactId>spark-mllib_${scala.version}</artifactId>
              <version>${spark.version}</version>
          </dependency>
  
      </dependencies>
  
      <build>
          <plugins>
  
              <plugin>
                  <groupId>org.scala-tools</groupId>
                  <artifactId>maven-scala-plugin</artifactId>
                  <version>2.15.2</version>
                  <executions>
                      <execution>
                          <goals>
                              <goal>compile</goal>
                              <goal>testCompile</goal>
                          </goals>
                      </execution>
                  </executions>
              </plugin>
  
              <plugin>
                  <groupId>org.apache.maven.plugins</groupId> <!--不加group id下面的version和artifact id会报错-->
                  <artifactId>maven-compiler-plugin</artifactId> 
                  <version>3.6.0</version>
                  <configuration>
                      <source>1.8</source>
                      <target>1.8</target>
                  </configuration>
              </plugin>
  
              <plugin>
                  <groupId>org.apache.maven.plugins</groupId>
                  <artifactId>maven-surefire-plugin</artifactId>
                  <version>2.19</version>
                  <configuration>
                      <skip>true</skip>
                  </configuration>
              </plugin>
  
          </plugins>
      </build>
  ```

  - 在项目的`src`文件夹内新建一个scala文件夹并右键`mark directory as source roots`，在此文件夹内新建`Scala Class`并选择`Object`即可开始编写Scala程序了~

 :blue_heart: **Hint：** 解决Spark运行过程中很多输出的问题 

  将`spark/conf/log4j.properties.example`拷贝到项目的`Source Root`下面并将`log4j.rootCategory`的参数由`INFO`改为`ERROR`(也可保留为`WARN`)重新运行就没有annoying的一大堆红色输出了（每次看到红色提示就算不是ERROR也会虎躯一震...）
  
- 数据处理及`Scala`程序设计逻辑同MapReduce
- 其中收藏及加入购物车的去重操作使用两次map实现（可以使用`distinct`但由于数据集本身较大，书上说调用`distinct`会进行数据混洗，因此通过两次map实现）
- RDD转化流程大致为：`filter`分别得到收藏/加入购物车的log数据，生成`((user_id,item/merchant_id),1)`的键值对并进行Reduce（利用计数进行聚合），再对去重之后的`item_id/merchant_id`进行reduceByKey即可
- 结果见[ProductSpark/Top100Item](https://github.com/HannahFromAland/FBDP/blob/main/PurchaseDataAnalysis/ProductSpark/Top100ItemSpark) 及[ProductSpark/Top100Merchant](https://github.com/HannahFromAland/FBDP/blob/main/PurchaseDataAnalysis/ProductSpark/Top100Merchant)

## Spark SQL

### 查询双十一购买商品的男女比例

```scala
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
      // 1.定义连接表达式
    val joinExpression = df_log.col("user_id") === df_info.col("user_id")
      // 2.左外连接
    val df_all = df_log.join(df_info, joinExpression, "left_outer").where("time_stamp = '1111' and action_type = '2' and gender in (0,1)" )
    df_all.groupBy("gender").count().show()
  }
}
```

- 返回结果

```bash
+------+------+
|gender| count|
+------+------+
|     0|846054|
|     1|323725|
+------+------+
# 323725/846054 = 0.38262924 与Hive查询结果一致
```

### 查询购买了商品的买家年龄段的比例

```scala
import org.apache.spark.sql.SparkSession

object CustomerAge {
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
    val df_all = df_log.join(df_info, joinExpression, "left_outer").where("time_stamp = '1111' and action_type = '2' and age_range in (1,2,3,4,5,6,7,8) and gender in (0,1)" )
    df_all.groupBy("age_range").count().sort("age_range").show()
  }
}
```

- 返回结果

```bash
+---------+------+
|age_range| count|
+---------+------+
|        1|    54|
|        2|122476|
|        3|314465|
|        4|252285|
|        5|127801|
|        6|101901|
|        7| 18868|
|        8|  3323|
+---------+------+
```


## Hive

- 首先配置`hive`并配置`MYSQL`作为元数据库

> 参考链接：[安装及配置hive](http://dblab.xmu.edu.cn/blog/1080-2/)

- 将数据导入hive并建立两张表`user_log` `user_info`

  - 将数据集上传至HDFS，并分别保存在`user_log`及`user_info`的文件夹中
  - 在hive中建表

  ```bash
  hann@ubuntu:~$ service mysql start
  hann@ubuntu:~$ hive
  hive>  create database dbtaobao;
  hive>  use dbtao;
  hive> CREATE EXTERNAL TABLE taobao.user_log(
  user_id INT,item_id INT,cat_id INT,merchant_id INT,brand_id INT,time_stamp INT,action_type INT)
  COMMENT 'create user_log!' 
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
  STORED AS TEXTFILE LOCATION '/user/hann/user_log';
  OK
  Time taken: 0.303 seconds
  hive> CREATE EXTERNAL TABLE taobao.user_info(
  user_id INT,age_range INT, gender INT) 
  COMMENT 'create user_info!' 
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
  STORED AS TEXTFILE LOCATION '/user/hann/user_info';
  OK
  Time taken: 0.177 seconds
  hive> select * from user_log limit 10;
  OK
  328862	323294	833	2882	2661	829	0
  328862	844400	1271	2882	2661	829	0
  328862	575153	1271	2882	2661	829	0
  328862	996875	1271	2882	2661	829	0
  328862	1086186	1271	1253	1049	829	0
  328862	623866	1271	2882	2661	829	0
  328862	542871	1467	2882	2661	829	0
  328862	536347	1095	883	1647	829	0
  328862	364513	1271	2882	2661	829	0
  328862	575153	1271	2882	2661	829	0
  Time taken: 2.257 seconds, Fetched: 10 row(s)
  
  ```

### 查询双十一当天购买的男女比例

> 考虑去重问题：由于双十一当天购买总量是按照**购买次数**作为总数进行统计的，因此对同一用户进行的多次购买操作不应该去重（极端情况为仅有一男一女用户，分别购买3次和4次某商品，实际按照购买总数进行性别统计的比例应为3:4而不是1:1）
> 
```bash
hive> select sum(case when c.gender=1 then 1 else 0 end)/sum(case when c.gender=0 then 1 else 0 end)
    > from(select a.user_id,a.action_type,
    > b.gender
    > from(select *
    > from user_log
    > where time_stamp=1111
    > and action_type=2)a
    > left outer join
    > (select *
    > from user_info)b
    > on a.user_id = b.user_id
    > where gender in (1,0))c;
    
OK
0.3826292411595477 # 双十一当天购买商品的男女比例
Time taken: 43.972 seconds, Fetched: 1 row(s)

```


> 首次运行出现报错：
>
> ```bash
> FAILED: Execution Error, return code 3 from org.apache.hadoop.hive.ql.exec.mr.MapredLocalTask
> ```
>
> 解决方案：
>
> ```bash
> hive (default)> set hive.auto.convert.join=false;
> #关闭mapjoin，即不对文件进行mapjoin，
> 因为mapjoin会首先将较小的一张表读入hashtable，再进行join工作，
> 禁用mapjoin之后可以解决内存溢出的问题，但直接使用join可能速度会变慢.
> ```
>

### 查询购买商品买家年龄段的比例

```bash
hive> select c.age_range, count(user_id)
    > from(select a.user_id,a.action_type,
    > b.age_range
    > from(select *
    > from user_log
    > where time_stamp=1111
    > and action_type=2)a
    > left outer join
    > (select *
    > from user_info)b
    > on a.user_id = b.user_id
    > where b.age_range in (1,2,3,4,5,6,7,8)
    > and b.gender in (0,1))c
    > group by c.age_range;
OK
1	54 #  0.0057%
2	122476 
3	314465
4	252285
5	127801
6	101901
7	18868
8	3323
total 941173
Time taken: 93.262 seconds, Fetched: 8 row(s)
```

- 比例结果如下
![双十一消费者年龄分布.png](https://i.loli.net/2020/12/21/yVwqNKpvck7sm5t.png)

## 预测回头客

### 数据预处理

- 首先对测试集中`label`进行去空处理，合并`user_info`并查看缺失值个数

```bash
(0,260864) #user_id
(0,260864) #merchant_id
(0,260864) # label
(1253,260864) #age_range
(3711,260864) #gender
```

- **去除缺失值：**考虑到数据集中能够表征用户信息的只有这两个变量，如果盲目采取各类缺失值填充方式都可能会对数据集的训练效果产生过大的影响，首先想到直接删除有缺失的数据，但是删除后发现有效数据仅剩196411（心痛），决定先使用该结果进行训练

> 网上求索时发现的一种不用改变数据原始特征形态的缺失值处理方法，马住。![image.png](https://i.loli.net/2020/12/25/npbjGgJIezEruZd.png)
>
> 出处：[机器学习中如何处理缺失数据](https://www.zhihu.com/question/26639110)

- 利用`user_log`进行特征工程构建

- 想要添加的特征：

  - 从用户侧考虑：
  
- **用户在该商家购买的次数**：是最直接的特征，由于每次购买均有成本且表明用户的强烈回购意愿，因此采用绝对数值作为特征值
  - 用户添加该商家商品入购物车的次数/该用户全部加入购物车的操作次数（同理构建加入收藏夹的特征及点击的特征）：将成本较低的几类操作行为对应商家所占比例用来表征用户对该商家商品的关注度，为避免各个用户使用习惯和操作的差异，取其对该商户进行的关注操作占其总关注行为的比例作为特征值
  - 用户在商家复购的种类：需要注意的是，预测回头客行为是基于目前所给定的全部用户数据对下一次行为进行预测（即对进行n次购物`n>=1`的用户的`n+1`次行为进行预测，所以用户在商家复购的种类越多，复购的商品种类越多，复购概率越大
  
- 从商家侧考虑：

  - 商家提供商品的种类数（商品种类越多，用户复购的选择空间越大，且可以与用户侧复购种类指标结合判断用户对商家的信任程度）
  - 商家直接竞争者个数（售卖同品牌同种类商品的商家个数）
  - 商家对应的消费者画像（年龄及性别）：使用用户数据与商家对应画像之间的差额来作为特征值

### 特征工程构建
- 利用Spark SQL进行特征工程构建（过程及其麻烦且出现很多意想不到的情况，果然90%的时间都用在特征工程。。）
  - 处理过程见`trainFeatureAdd` 以及`finalFeature`，此处mark几个常用到的操作

  ```scala
  // join
  val title_add1 = df_title.join(re_pur, df_title.col("user_id") === re_pur.col("user_id") and
        df_title.col("merchant_id") === re_pur.col("merchant_id"), "left_outer").select(df_title("user_id"),
        df_title("merchant_id"), re_pur("count").as("pur"))
  // count missing value
  val columns=test_info.columns
  val missing_cnt=columns.map(x=>test_info.select(col(x))
                              .where(col(x).isNull).count)
  missing_cnt.foreach(println)
  
  // filling missing values with exact value
  val tmp5 = tmp4.na.fill(0)
  
  // filling missing values with column average
  val c2 = c1.withColumn("click_fill", when($"click".isNull, c1.select(mean("click"))
        .first()(0).asInstanceOf[Double])
        .otherwise($"click"))
  
  // add new column from existing calculation
  val mean_mer = res51.groupBy("merchant_id")
  .agg(avg("age_range").as("mean_age"), avg("gender")
       .as("mean_gender")).show()
  
  
  ```

  

  - 最终构建出的特征工程数据集结构：

![image.png](https://i.loli.net/2020/12/26/uvCNGXsmMQeg5AJ.png)

### ML训练及模型选择

选择ML库进行训练（觉得pipeline好玩所以想尝试一下，但是发现单纯调用分类器在本题中好像不需要pipeline，于是在挑选分类器时直接调用了模型，后面使用交叉验证时才用上心心念念的pipeline）

具体代码见`TrainMl`

- **LogisticRegression:**

```scala
------------------------LogisticRegression---------------------------

Accuracy: 0.938821070018324
weightedPrecision: 0.8813850015103508
weightedRecall: 0.938821070018324
areaUnderROC: 0.5
areaUnderPR: 0.530589464990838

```

AUC的结果真的是差到惊人...于是尝试使用5折交叉验证改进一下结果

```scala
------------------------CV: LogisticRegression---------------------------

Accuracy: 0.9376287791413428
weightedPrecision: 0.8792183605350958
weightedRecall: 0.9376287791413428
areaUnderROC: 0.6027514632991644
areaUnderPR: 0.10143564602283778
BestRegParam:0.01
```

- RandomForest

```scala
------------------------RandomForest---------------------------

Accuracy: 0.9373279749828782
weightedPrecision: 0.8785837326855032
weightedRecall: 0.9373279749828782
areaUnderROC: 0.6241610403165713
areaUnderPR: 0.10948325671524473
```

使用5折交叉验证训练RandomForest

```scala
------------------------CV: RandomForest---------------------------

Accuracy: 0.9396066322426633
weightedPrecision: 0.8828842681155552
weightedRecall: 0.9396066322426633
areaUnderROC: 0.6346419331989969
areaUnderPR: 0.10854576073190937
BestMaxDepth:10
BestNumTrees:20
```
- 使用随机森林对testdata进行测试，最终得分为`score:0.6229398`

>[Spark ML机器学习库评估指标示例](https://zhuanlan.zhihu.com/p/110664865)
>[决策树分类器 -- spark.ml](http://mocom.xmu.edu.cn/article/show/58667ae3aa2c3f280956e7b0/0/1)
>[海内存知己的debug，不得不说Stack Overflow yyds](https://stackoverflow.com/questions/64114103/spark-illegalargumentexception-column-features-must-be-of-type-structtypetiny)
>[park ML Pipeline模型选择及超参数评估调优深入剖析 -Spark商业ML实战](https://blog.csdn.net/shenshouniu/article/details/84197012)
>[将预测结果转为string并输出到csv](https://stackoverflow.com/questions/40426106/spark-2-0-x-dump-a-csv-file-from-a-dataframe-containing-one-array-of-type-string%20%22here%22%20on%20Stackoverflow)


