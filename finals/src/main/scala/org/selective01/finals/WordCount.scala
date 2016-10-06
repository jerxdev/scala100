package org.selective01.finals

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  
  def main(args: Array[String]) = {
    
  // System.setProperty("hadoop.home.dir", "C:\\winutil\\");
    
   
   //Start the Spark context //jerx
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)
    
     //Read file  //jerx
    val inputfile = sc.textFile("input.txt")
    inputfile.flatMap (_.split(" "))
    
    
    // .filter(x => x.matches("[a-zA-Z-0-9]+")) //jillah revised (allow symbols)
    .map { word =>  //for each word
      ( word , 1 ) } //return a key/value tuple, with the word as key and 1 as value
    
    .reduceByKey(_ + _)
    .sortByKey()//jillah revised (arrange in alphabetical order)
    .saveAsTextFile("input.result.txt")
    
    
    
    //Stop the Spark context
    sc.stop
  }
}
