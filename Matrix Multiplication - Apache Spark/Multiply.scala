package edu.uta.cse6331

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Multiply {
  def main(args: Array[ String ]) {
  	 val conf = new SparkConf().setAppName("Multiply")
  	 val sc = new SparkContext(conf)
  	 val M = sc.textFile(args(0)).map( line => { val a = line.split(",")
                                                (a(1).toInt, (a(0).toInt,a(2).toDouble)) } )
  	 val N = sc.textFile(args(1)).map( line => { val a = line.split(",")
                                                (a(0).toInt, (a(1).toInt,a(2).toDouble)) } )


  	 val res = M
  	   						.join(N)
    						.map({ case (_, ((i, v), (k, w))) => ((i, k), (v * w)) })
    						.reduceByKey(_ + _)
    						.sortByKey()
    						.map({ case ((i, k), sum) => i+ "," + k + "," + sum })

    res.coalesce(1)saveAsTextFile(args(2))
    sc.stop()
  }
}
