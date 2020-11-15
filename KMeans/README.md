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

输入分类结果文件路径，即可在python文件路径下获取可视化结果图片（最多支持分10类）。

## 聚类结果

实验将数据点分别聚为3类，5类，10类，每种聚类进行了三次，因聚类最初随机选取的中心点不同，聚类结果也会不同。但总体来看，聚的类数越多，聚类结果越稳定。

#### K=3

![](https://i.loli.net/2019/11/11/rpBJh4zgn7OuqbN.png)

![](https://i.loli.net/2019/11/11/pIxRdlY7eiZSEK1.png)

![](https://i.loli.net/2019/11/11/kK5JP4XFdGhWYBA.png)

#### K=5

![](https://i.loli.net/2019/11/11/6TSJ8eLfIxvd2b3.png)

![](https://i.loli.net/2019/11/11/7B4mgoUXul8CKyH.png)

![](https://i.loli.net/2019/11/11/7DBFrVEJsLnyTbA.png)

#### K=10

![](https://i.loli.net/2019/11/11/eoD76QLfdrhb4W3.png)

![](https://i.loli.net/2019/11/11/FEH5qtcsbzyxYa3.png)

![](https://i.loli.net/2019/11/11/VRvAM3K74ZhSgTE.png)

#### Conclusion

可以看出，聚为3类时聚类结果很不稳定，5类时稍好，10类时聚类结果比较稳定。

