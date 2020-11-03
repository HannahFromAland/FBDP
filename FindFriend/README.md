# MapReduce FindCommonFriends

**Definition：**< person> , < friend1>< friend2 >…< friendn >

friend1和friend2的共同好友不一定含有person，该list只能说明person作为friend的好友出现，因此要首先对给出的friend list进行反向处理，保证value组中的两两组合共同好友即为key对应的用户。

### 基本数据类型实现 

即利用int及IntWritable类型读入用户名

- MapReduce job1实现

  - Map：读入数据 

    eg. 100, 200	300	400	500	600

    ​	   200, 100	300	400

    <key,value>:<200, 100> <300, 100><400, 100>...

  - 此时的键值对含义为：key值用户的好友中都有value值用户

  - Reduce

  -  通过查看样例发现，**好友关系是单向的**（即100的好友列表中没有600，但600的好友列表中有100，此时100和600的任意一好友共同好友则不能为600），因此reduce阶段就是将题目中的好友列表反向读入，保证value中用户的好友中都有key中的用户

    eg. 100, 200	300	400	500	600
    
    ​	  200, 100	300	400
    
    <key,value>: <300, 100> <300, 200> 在reduce中规约为 300 100,200
    
    通过第一步mapreduce实现用户列表反转之后，才可以保证遍历value中的两两用户，其共同好友为key对应的用户

- MapReduce job2实现
  - Map：读入上一步的reduce数据 在value中形成两两组合的用户对并输出其共同好友

  ​       key: [200,300] value: 100, 400

  - Reduce： 合并value

  ​       key: [200,300] value: [100, 400]

- 最终结果见output/part-r-00000