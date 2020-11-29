#### 1. 简述Spark的技术特点

- Spark运算速度：与Hadoop的MapReduce相比，Spark基于内存的运算要快100倍以上，基于硬盘的运算也要快10倍以上。Spark实现了高效的DAG执行引擎，减少了迭代过程中数据的落地，提高了处理效率；
- Spark语言支持：支持Java、Python、Scala和R的API，还支持超过80种高级算法，使用户可以快速构建不同的应用。而且Spark支持交互式的Python和Scala的shell，可以非常方便地在这些shell中使用Spark集群来验证解决问题的方法；
- Spark生态：提供完整的技术栈，包括批处理、SQL查询、流式计算、机器学习和图算法组件，可以通过同一平台完成众多数据需求；
- Spark容错性：Spark提出的RDD弹性分布式数据集，是Spark最核心的分布式数据抽象。在RDD 算时可以通过CheckPoint来实现容错。
- Spark调度：采用事件驱动的Scala库类Akka来完成任务启动，通过复用线程池的方式来取代MapReduce进程或者线程启动和切换的开销。
- Spark部署：以非常方便地与其他的开源产品进行融合。比如，Spark可以使用Hadoop的YARN和Apache Mesos作为它的资源管理和调度器，并且可以处理所有Hadoop支持的数据，包括HDFS、HBase和Cassandra等。对于已经部署好的Hadoop集群，不需要任何数据迁移即可转而使用spark进行数据处理。Spark也可以不依赖于第三方的资源管理和调度器，它实现了Standalone作为其内置的资源管理和调度框架，这样进一步降低了Spark的使用门槛，同时大大提升了部署和使用Spark的效率。
- Spark数据操作及通信模型：Hadoop只提供了Map和Reduce两种操作， 而Spark提供的数据集操作类型很多。另外各个处理节点之间的通信模型不再像Hadoop只有Shuffle一种，用户可以命名、物化、控制中间结果的存储、分区等。

#### 2. 简述Spark的基本构架和组件功能

Spark 的各个组件如图所示：![image.png](https://i.loli.net/2020/11/29/DJLHNkSKhqT7Me8.png)



**Spark Core**
Spark Core 实现了 Spark 的基本功能，包含任务调度、内存管理、错误恢复、与存储系统交互等模块。Spark Core 中还包含了对弹性分布式数据集（resilient distributed dataset，简称 RDD）的 API 定义。RDD 表示分布在多个计算节点上可以并行操作的元素集合，是Spark 主要的编程抽象，提供有向无环图DAG的分布式并行计算框架，并提供 Cache机制来支持多次迭代计算或者数据共享。Spark Core 提供了创建和操作这些元素集合的多个 API。

**Spark SQL**
Spark SQL 是 Spark 用来操作结构化数据的程序包。Spark SQL 支持多种数据源，比如 Hive 表、Parquet 以及 JSON 等。除了为 Spark 提供了一个 SQL 接口，Spark SQL 还支持开发者将 SQL 和传统的 RDD 编程的数据操作方式相结合，不论是使用 Python、Java 还是 Scala，开发者都可以在单个的应用中同时使用 SQL 和复杂的数据分析。此外Spark SQL也可查询在Hive上存在的外部数据，即统一关系表和RDD的处理，便于进行更复杂的数据分析工作。

**Spark Streaming**
Spark Streaming 是 Spark 提供的对实时数据进行流式计算的组件，可以对多种数据源（如Kafka、 Flume、Twitter、Zero和TCP 套接字）进行类似Map、 Reduce和Join等复杂操作，并将结果保存到外部文件系统、 数据库或应用到实时仪表盘。Spark Streaming 的工作机制是对数据流进行分片，使用 Spark计算引擎处理分片数据，并返回相应分片的计算结果。SparkStreaming 提供了用来操作数据流的 API，并且与 Spark Core 中的 RDD API 高度对应。从底层设计来看，Spark Streaming 支持与Spark Core 同级别的容错性、吞吐量以及可伸缩性。

**MLlib**
MLlib是Spark 中包含的机器学习程序库，提供了包括分类、回归、聚类、协同过滤等众多机器学习算法。此外，还提供了模型评估、数据导入等额外的支持功能。MLlib 还提供了一些更底层的机器学习原语，包括一个通用的梯度下降优化算法。所有这些方法都被设计为可以在集群上轻松伸缩的架构。

**GraphX**
GraphX是Spark中用于图和图并行计算的API。与 Spark Streaming 和 Spark SQL 类似，GraphX 也扩展了 Spark 的 RDD API，能用来创建一个顶点和边都包含任意属性的有向图。GraphX 还支持针对图的各种操作（比如进行图分割的 subgraph 和操作所有顶点的 mapVertices），以及一些常用图算法（比如 PageRank和三角计数）。就底层而言，Spark 设计为可以高效地在一个计算节点到数千个计算节点之间伸缩计算。

**独立调度器**

为了Spark的计算和处理要求，同时获得最大灵活性，Spark 支持在各种集群管理器（clustermanager）上运行，包括 Hadoop YARN、Apache Mesos，以及 Spark 自带的一个简易调度器，叫作独立调度器。

#### 3. 简述何为“数据编排”以及Alluxio的特点

数据编排是指将跨平台及系统的数据访问抽象并进行虚拟化，最终通过具有全局命名空间的标准化API将数据呈现给数据驱动的应用程序进行处理的统一操作。数据编排平台架构于计算框架和存储系统之间，具有能够及时访问热数据的缓存功能，为数据驱动应用程序提供数据的可访问性、本地性及可伸缩性。

**Alluxio的主要特点：**

- Alluxio可用作分布式共享缓存服务，因此与Alluxio通信的计算应用程序可以透明地缓存经常访问的数据，尤其是来自远程位置的数据，以提供内存中的I/O吞吐量。
- Alluxio采用简化的云和对象存储：云和对象存储系统使用与传统文件系统相比具有性能影响的不同语义。
- 数据存储与计算分离，两部分引擎可以进行独立的扩展。 计算引擎可以访问不同数据源中的数据。
- 简化数据管理：Alluxio提供对多个数据源的单点访问。除了连接不同类型的数据源之外，Alluxio还使用户能够同时连接到同一存储系统的不同版本，例如多个版本的HDFS，而无需复杂的系统配置和管理。
- 应用程序部署：Alluxio管理应用程序与文件或对象存储之间的通信，将数据访问请求从应用程序转换为底层存储接口。Alluxio兼容Hadoop，Spark和MapReduce程序，可以无需修改任何代码在Alluxio之上运行。