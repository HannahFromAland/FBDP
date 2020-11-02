# MapReduce FindCommonFriends

- MapReduce实现
  - Map：读入数据 

    key: 100 value: 200, 300, 400, 500, 600

  - Reduce： 遍历value可以得到任意两个对应的共同好友

    key: [200,300] value: 100; key: [200,400] value: 100

- MapReduce实现

  - Map：读入上一步的reduce数据 并去重（要求key里面元素1<元素2）

    key: [200,300] value: 100, 400

  - Reduce： 合并value

    key: [200,300] value: [100, 400]





