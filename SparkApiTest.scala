package com.meituan.hotel.data.learningspark.example.util
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Author: niepengyu@meituan.com
  * Date: 15/12/20
  * Created by niepengyu on 15/12/20.
  */

/*
transformation操作：
map(func):对调用map的RDD数据集中的每个element都使用func，然后返回一个新的RDD,这个返回的数据集是分布式的数据集
filter(func) : 对调用filter的RDD数据集中的每个元素都使用func，然后返回一个包含使func为true的元素构成的RDD
flatMap(func):和map差不多，但是flatMap生成的是多个结果
mapPartitions(func):和map很像，但是map是每个element，而mapPartitions是每个partition
mapPartitionsWithSplit(func):和mapPartitions很像，但是func作用的是其中一个split上，所以func中应该有index
sample(withReplacement,faction,seed):抽样
union(otherDataset)：返回一个新的dataset，包含源dataset和给定dataset的元素的集合
distinct([numTasks]):返回一个新的dataset，这个dataset含有的是源dataset中的distinct的element
groupByKey(numTasks):返回(K,Seq[V])，也就是hadoop中reduce函数接受的key-valuelist
reduceByKey(func,[numTasks]):就是用一个给定的reduce func再作用在groupByKey产生的(K,Seq[V]),比如求和，求平均数
sortByKey([ascending],[numTasks]):按照key来进行排序，是升序还是降序，ascending是boolean类型
join(otherDataset,[numTasks]):当有两个KV的dataset(K,V)和(K,W)，返回的是(K,(V,W))的dataset,numTasks为并发的任务数
cogroup(otherDataset,[numTasks]):当有两个KV的dataset(K,V)和(K,W)，返回的是(K,Seq[V],Seq[W])的dataset,numTasks为并发的任务数
cartesian(otherDataset)：笛卡尔积就是m*n，大家懂的



action操作：
reduce(func)：说白了就是聚集，但是传入的函数是两个参数输入返回一个值，这个函数必须是满足交换律和结合律的
collect()：一般在filter或者足够小的结果的时候，再用collect封装返回一个数组
count():返回的是dataset中的element的个数
first():返回的是dataset中的第一个元素
take(n):返回前n个elements，这个士driver program返回的
takeSample(withReplacement，num，seed)：抽样返回一个dataset中的num个元素，随机种子seed
saveAsTextFile（path）：把dataset写到一个text file中，或者hdfs，或者hdfs支持的文件系统中，spark把每条记录都转换为一行记录，然后写到file中
saveAsSequenceFile(path):只能用在key-value对上，然后生成SequenceFile写到本地或者hadoop文件系统
countByKey()：返回的是key对应的个数的一个map，作用于一个RDD
foreach(func):对dataset中的每个元素都使用func
*/
object SparkApiTest {

  def myfunc[T](iter: Iterator[T]) : Iterator[(T, T)] = {
    var res = List[(T, T)]()
    var pre1 = iter.next;
    while (iter.hasNext) {
      val cur = iter.next;
      res .::= (pre1, cur)
      pre1 = cur;
    }
    res.iterator
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("RunSparkExample")
    val sc = new SparkContext( sparkConf )
    /*
    * 1. Generate RDD
    *    Three diffierent ways to generate RDD
    * */
//    val rddtest=sc.textFile("test");
    val rddarray=sc.makeRDD(List("1","2","3","4","5","6"));
    val a = sc.parallelize(1 to 9, 3)

// Transformation

    /*
    * 1. Map: Apply functions to each line
    * */
    val testrdd2=sc.parallelize(List("abc 1", "def 2", "hijk 3"));
    val testrdd3 = testrdd2.map{case line=>val  tokens=line.split(" ");(tokens(0),tokens(1))}
    val testrdd4=testrdd2.map(_+" append");

    /*
    * Filter: Apply functions to each line, and gets lines that check is true
    * */
    val testrdd5=testrdd2.filter(_.split(" ")(0)=="abc")
    val testrdd6=testrdd2.map(_.split(" ")).filter(x => (x(0)=="abc"));

    /*
    * flatMap: Process each line to many lines, and generate new RDD
    * Sample: wordCount
    * */
    val testrdd7=testrdd2.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_+_);

  /*
  * MapPartitions: Apply Input functions to each partitions
  * Sample: Array((2,3), (1,2), (5,6), (4,5), (8,9), (7,8))
  * */
    val testa = sc.parallelize(1 to 9, 3);
    println(testa.mapPartitions(myfunc).collect);

  /*
  * mapValues: Apply Input functions to each value
  * Sample: Array((3,xdogx), (5,xtigerx), (4,xlionx), (3,xcatx), (7,xpantherx), (6,x eaglex))
  * */
    val testrdd8 = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", " eagle"), 2).map(x => (x.length, x)).mapValues("x" + _ + "x").collect
    println("Hello")

    /*
   * flatMapWith: Contains two functions
   *          First function takes partition index ad input
   *          Second function takes (partitions index, line) as input
   *          Finally puts each output as a RDD line
   * Sample: Array[Int] = Array(0, 1, 0, 2, 0, 3, 1, 4, 1, 5, 1, 6, 2, 7, 2, 8, 2, 9)
   * */
    val x = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10), 3)
    x.mapWith(a => a * 10)((a, b) => (a+b+2)).collect



    /*
  * reduceByKey: Process reduce on values with the same key
  * Sample: Array((1,2), (3,10))
  * */
    val testrdd11 = sc.parallelize(List((1,2),(3,4),(3,6)))
    testrdd11.reduceByKey((x,y) => x + y).collect

    /*
  * SortBy: Takes input, and process compare using selected field
  * Sample: Array(Array(hij, 1), Array(def, 2), Array(abc, 3))
  * */
    val testrdd12=sc.makeRDD(List("abc 3","def 2","hij 1"))
    val testrdd13=testrdd12.map(_.split(" ")).sortBy(x=>x(0),false).collect;

    /*
  * SortByKey: Takes input, and process compare using Key
  * Sample: Array((1,10), (2,9), (3,8), (4,7), (5,6), (6,5), (7,4), (8,3), (9,2), (10,1))
  * */
    val testrdd15 = sc.parallelize(1 to 10).map(x=> (x,11-x));
    val testrdd16=testrdd15.sortByKey();

    /*
  * Union: Generate a new value using two values, and loop use this new value with left values
  * Sample: 55
  * takeSample:   withReplacement, num , seed
  *
  * cartesian: full join
  * */
    val testrdd17=sc.makeRDD(List("abc 3","def 2","hij 1")).map{line => val tokens=line.split(" ");(tokens(0),tokens(1))}
    val testrdd18=sc.makeRDD(List("abc 4","def 5","klm 6")).map{line => val tokens=line.split(" ");(tokens(0),tokens(1))}
    val testrdd19=testrdd17.join(testrdd18);
    val testrdd20=testrdd17.union(testrdd18);
    val testrdd21=testrdd17.distinct();
    val testrdd22=testrdd17.groupByKey();
    val testrdd23=testrdd17.cartesian(testrdd18);

    /*
  * Reduce: Generate a new value using two values, and loop use this new value with left values
  * Sample: 55
  * takeSample:   withReplacement, num , seed
  * */
    val testrdd14=sc.parallelize(1 to 10);
    val value1=testrdd14.reduce((x, y) => x + y)
    val value2=testrdd14.reduce(_+_)
    val count1=testrdd14.count();
    val testcollect=testrdd14.collect();

    val testresult=testrdd14.takeSample(true,2,3);

//    testrdd14.saveAsSequenceFile("");
    testrdd14.saveAsTextFile("filepath");
  }
}
