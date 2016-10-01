package org.selective01.finals

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  
  def main(args: Array[String]) = {
    
   // System.setProperty("hadoop.home.dir", "C:\\winutil\\"); >> this is just for local machine
    
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)
    
    val inputfile = sc.textFile("input.txt")
    inputfile.flatMap (_.split(" "))
    .filter(x => x.matches("[a-zA-Z-0-9]+"))
    
    .map { word =>
      ( word , 1 )
     
    }
    
    .reduceByKey(_ + _)
    .saveAsTextFile("input.result.txt")
    
    sc.stop
  }
}
