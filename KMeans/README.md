# 作业7 利用MapReduce实现Clustering

实现了Kmeans求聚类结果和结果的可视化。

#### 求聚类结果：
参考代码为Chapter10 KMeans

主要参数：

* num of k：一共要聚成几类(int)
* inputpath：数据点文件(filepath)
* outputpath：cluster中间结果和最终分类结果的目录(dirpath)
* iteration times：迭代次数(int)

执行语句：
> $ bin/hadoop jar KMeansResult.jar \<num of k> \<inputpath> \<outputpath> \<iteration times>



#### 聚类结果可视化：

## 聚类结果

实验将数据点分别聚为3类，5类，10类，每种聚类按照其类别进行相应次数的迭代，因聚类最初随机选取的中心点不同，聚类结果也会不同。但总体来看，聚的类数越多，聚类结果越稳定。

#### K=3（设定迭代次数为3）

![k_3.png](https://i.loli.net/2020/11/16/nwSBR765DdsJI2j.png)

![k_3_2try.png](https://i.loli.net/2020/11/16/2A8vqfxhEcK4MeC.png)

![k_3_3try.png](https://i.loli.net/2020/11/16/Lr1JvdGCzyKqtmY.png)

#### K=5（设定迭代次数为3）

![k_5.png](https://i.loli.net/2020/11/16/4evNIrVoT8w9zlx.png)

![k_5_2try.png](https://i.loli.net/2020/11/16/h9O2HCWBbKE5TYy.png)

![k_5_3try.png](https://i.loli.net/2020/11/16/NxKps9Eq4DGFr1Q.png)

#### K=10（设定迭代次数为10）

![k_10.png](https://i.loli.net/2020/11/16/PoTEAUxH82XFgpe.png)

![k_10_2try.png](https://i.loli.net/2020/11/16/o6auTGUxdp1zqQt.png)

![k_10_3try.png](https://i.loli.net/2020/11/16/WQspaXZy3dO9eRx.png)

#### Conclusion

可以看出，聚为3类时聚类结果很不稳定，5类时稍好，10类时聚类结果比较稳定。
