package com.dr.core.wc

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[4]").setAppName("wordCount")
    val sc = new SparkContext(conf)

    val initRDD = sc.parallelize(List("hive", "hadoop"), 2)

    initRDD.map((_, 1)).reduceByKey(_ + _).foreach(println)

    sc.stop()
  }

}
