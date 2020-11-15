# 作业5 README

实现了Kmeans求聚类结果和结果的可视化。

result目录为运行结果和可视化图片。

## 运行说明

#### 求聚类结果：

在系统节点上执行如下命令：

* num of k：一共要聚成几类(int)
* inputpath：数据点文件(filepath)
* outputpath：cluster中间结果和最终分类结果的目录(dirpath)
* iteration times：迭代次数(int)

> $ bin/hadoop jar KMeansResult.jar \<num of k> \<inputpath> \<outputpath> \<iteration times>
>
> 例：
>
> $ bin/hadoop jar KMeansResult.jar 5  KMeans/input/NewInstance.txt  KMeans/output/  20
在output路径下的result目录中获取分类结果

#### 可视化：

运行聚类数据可视化.py

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

