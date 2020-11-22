### 安装HBase && 单机实现

- 查阅之前的参考资料及Apache官网给出的QuickStart发现基本步骤和Hadoop类似，在官网上选了**2.2.6版本**进行安装（期待能一次成功）

![image.png](https://i.loli.net/2020/11/19/xKUsASjNYFqHytk.png)

- 修改 `/etc/profile` 环境变量文件，添加 Hbase 的环境变量，追加下述代码

```
export HBASE_HOME=/usr/local/hbase
export PATH=$PATH:$HBASE_HOME/bin
```

- 使环境变量配置文件生效 `# source /etc/profile`
- 在目录 /usr/local/hbase/conf 修改配置
1. 修改 hbase-env.sh，追加
```
export JAVA_HOME=/usr/lib/jvm/jdk1.8.0_261
export HBASE_MANAGES_ZK=true
```

2. 在*conf/hbase-site.xml*添加配置：

![image.png](https://i.loli.net/2020/11/19/GtfXe3UZ2RioDLJ.png)

问题解决：

1.hbase的启动顺序应该是hadoop ->zookeeper -> hbase

2.权限不够 需要更改hbase文件夹的权限

```
sudo chmod -R 777./hbase
```



![image.png](https://i.loli.net/2020/11/19/TXmo2Ysndh1EfZy.png)

3.上网搜索后发现包下错了（全网居然还有能解答我这个问题的人），应该选择bin而不是src

> src文件为未编译文件，bin为二进制文件，应该下载bin

![image.png](https://i.loli.net/2020/11/19/m7MhblQe3GAHDcB.png)

- 重新下载bin文件之后重复上述步骤成功！

1. 查看jps

2. 通过默认web端口 *[http://localhost:16010](http://localhost:16010/)* 来查看Web UI

![image.png](https://i.loli.net/2020/11/19/md3vDg7tJjAaTir.png)

- 开启hbase 服务及hbase shell

```
bin/start-hbase.sh
```

- 试用hbase shell

```
$./bin/hbase shell
hbase(main):001:0>
```

- 报错：

![image.png](https://i.loli.net/2020/11/20/yHsZklNKG6me7qY.png)

查阅别人的报错经历+官网教程发现单节点quickstart好像只需要对env中的java home进行配置，其余都暂时不需要

> ```
> export JAVA_HOME=/usr/lib/jvm/jdk1.8.0_261
> export HBASE_MANAGES_ZK=true #这句是用来配置hbase对zookeeper的管理，甚至可以不加
> true表示自己管理，false表示自行重新安装zookeeper
> ```

> *conf/hbase-site.xml* 中已有配置用于指定hbase数据的存储位置，默认的./tmp在每次重启时都会丢失数据，建议修改为**/usr/local/habase/tmp** --从hadoop获得的灵感

![image.png](https://i.loli.net/2020/11/20/vk84wT6uIzVXg5y.png)

- 调整后重新对hbase shell进行启动和试用，成功~

![image.png](https://i.loli.net/2020/11/20/24kfADE9mGTRgXj.png)



### 伪分布式

1. 首先修改*conf/hbase-site.xml*  --这里参考官网tutorial发现最终只需要保留两个property（和网上五花八门的教程形成鲜明对比），于是决定先进行尝试

```xml
<property>
  <name>hbase.cluster.distributed</name>
  <value>true</value>
</property>
<property>
  <name>hbase.rootdir</name>
  <value>hdfs://localhost:9000/hbase</value>
</property>
#删除原有的hbase.tmp.dir 和hbase.unsafe.stream.capability.enforce配置即可
```

2. 之后重新启动hbase，查看jps发现jps中除了hadoop守护程序，还要有`HMaster`, `HRigionServer`,`HQuorumPeer`这三个，代表hbase和zookeeper都已开启。

![image.png](https://i.loli.net/2020/11/20/Cc2pwjJxbkMfVqR.png)

3. 打开hbase shell进行测试

   ```
   $ bin/start-hbase.sh
   $ bin/hbase shell
   ```

   能够成功执行建立表格、添加数据等操作就可以了。

![image.png](https://i.loli.net/2020/11/20/lMvFCZqNeLxGog7.png)



### 集群实现

在之前利用docker搭建的hadoop集群中实现

首先对之前的hadoop的容器进行修改（之前建了5个节点导致卡住启动不了，后来投奔bdkit，刚好借此机会减少节点数进行尝试）

改为2节点之后运行wordcount进行测试结果如下：

![image.png](https://i.loli.net/2020/11/20/KWHyOCtfM4nQ1Tb.png)



#### 配置HBase集群

- 下载HBase 2.2.6

```text
root@h01:~# wget https://mirror.bit.edu.cn/apache/hbase/2.2.6/hbase-2.2.6-bin.tar.gz
```
- 解压到 /usr/local 目录下面并更名为hbase
```
root@h01:~# tar -zxvf hbase-2.2.6-bin.tar.gz -C /usr/local/
root@h01:/usr/local# mv ./habse-2.2.6 ./hbase

```
- 修改 /etc/profile 环境变量文件，添加 Hbase 的环境变量，追加下述代码并使环境变量配置生效
```
export HBASE_HOME=/usr/local/hbase
export PATH=$PATH:$HBASE_HOME/bin
```
```
root@h01:/usr/local# source /etc/profile
root@h01:/usr/local#
```
- 使用 ssh h02 可进入其他四个容器，依次修改。
  即每个容器都要在 /etc/profile 文件后追加那两行环境变量
  之后退回h01
- 在目录 `/usr/local/hbase-2.1.3/conf` 修改配置

修改 hbase-env.sh，追加

```text
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-i386
export HBASE_MANAGES_ZK=true
```

修改 hbase-site.xml 为

```xml
<configuration>
        <property>
                <name>hbase.rootdir</name>
                <value>hdfs://h01:9000/hbase</value>
        </property>
        <property>
                <name>hbase.cluster.distributed</name>
                <value>true</value>
        </property>
        <property>
                <name>hbase.master</name>
                <value>h01:60000</value>
        </property>
        <property>
                <name>hbase.zookeeper.quorum</name>
                <value>h01,h02</value>
        </property>
        <property>
                <name>hbase.zookeeper.property.dataDir</name>
                <value>/home/hadoop/zoodata</value>
        </property>
</configuration>
```

修改 `regionservers` 文件为

```text
h01
h02
```
- 使用 scp 命令将配置好的 Hbase 复制到其他 2 个容器中（虽然h03节点没有开，但为了防止之后忘了还是都统一配置了）

```bash
root@h01:~# scp -r /usr/local/hbase root@h02:/usr/local/
root@h01:~# scp -r /usr/local/hbase root@h03:/usr/local/
```

- 启动Hbase，通过jps查看当前启动情况

```bash
root@h01:/usr/local/hbase-2.1.3/bin# ./start-hbase.sh 
root@h01:/usr/local/hbase-2.1.3/bin# jps
```
- 打开Hbase的shell
- 但是发现一旦打开shell，Hmaster就会消失

![image.png](https://i.loli.net/2020/11/20/H2PyZOWC5inmJe8.png)

- 此处mark一个报错：

执行`./stop-hbase.sh`时再次运行jps发现无法关闭HRegionServer， 上网查阅解决方式

关闭HRegionServer(同在bin文件夹内)即可
``./hbase-daemon.sh stop regionserver RegionServer``

集群节点启动的尝试暂时失败（卑微），后续准备利用外置zookeeper进行尝试



### 运行students表

#### shell操作版

1. 建表 
2. scan

![image.png](https://i.loli.net/2020/11/21/qxN1IRMALVvGro4.png)

3. 查询学生来自的省 

4. 增加新的列Courses:English，并添加数据；

![image.png](https://i.loli.net/2020/11/22/diTt8Y3NHJGSQFE.png)

5. 增加新的列族Contact和新列Contact:Email，并添加数据
6. 删除students表

![image.png](https://i.loli.net/2020/11/22/Xn5vMuIaWytzeHo.png)