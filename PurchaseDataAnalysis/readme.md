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

- 首先需要通过MapReduce实现用户行为日志和用户画像的`join`操作（)

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
> - 而添加购物车和收藏夹的动作对同一用户来说多次进行是没有任何特殊业务意义（多次操作甚至在某种程度上代表购买欲望不强因此出现多次删除又加入购物车/收藏夹的情况x），因此决定对添加购物车和添加收藏夹的用户操作进行去重处理
> - 假设同一用户的购买和添加购物车/收藏夹操作之间不产生重复影响
> - 进一步优化方向（但因为感觉略不符合题意所以没实现hh只是对于该分析逻辑的想法）：对三种行为的计数进行加权，如`单次购买行为：单次添加购物车：单次添加收藏夹=50%：30%：20%`，可以有效区分被过多添加收藏夹/购物车的商品与购买次数多的商品之间的“受欢迎程度”

- 使用两次MapReduce实现：第一次MapReduce实现去重和频率统计，第二次MapReduce进行频率倒排（源文件分别为`Product/src/main/java/PopularItem/AllItem` 以及`Product/src/main/java/PopularItem/PopMerYoung` 

- 结果见`Product/PopularItem` 及`Product/PopularMerchant`

## 双十一购买商品的男女比例，以及购买了商品的买家年龄段的比例

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
  hive> CREATE EXTERNAL TABLE taobao.user_log(user_id INT,item_id INT,cat_id INT,merchant_id INT,brand_id INT,time_stamp INT,action_type INT) COMMENT 'create user_log!' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/hann/user_log';
  OK
  Time taken: 0.303 seconds
  hive> CREATE EXTERNAL TABLE taobao.user_info(user_id INT,age_range INT, gender INT) COMMENT 'create user_info!' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/hann/user_info';
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

- 查询双十一当天购买的男女比例

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
> hive (default)> set hive.auto.convert.join=false; #关闭mapjoin，即不对文件进行mapjoin，因为mapjoin会首先将较小的一张表读入hashtable，再进行join工作，禁用mapjoin之后可以解决内存溢出的问题，但直接使用join可能速度会变慢.
> ```
>

- 查询购买商品买家年龄段的比例

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
  
