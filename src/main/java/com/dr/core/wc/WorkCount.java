package com.dr.core.wc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WorkCount {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("WorkCount").setMaster("local[4]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initRDD = sc.parallelize(Arrays.asList("hadoop", "hive", "hbase", "spark", "flink", "flink"), 2);

        JavaPairRDD<String, Integer> map = initRDD.mapToPair(s -> new Tuple2<>(s, 1));

        map.reduceByKey((v1, v2) -> v1 + v2).foreach(stringIntegerTuple2 -> System.out.println(stringIntegerTuple2));


//        List<Tuple2<String, Integer>> collect = map.collect();
//
//        for (Tuple2<String, Integer> stringIntegerTuple2 : collect) {
//            System.out.println(stringIntegerTuple2);
//        }

        sc.stop();
    }
}
