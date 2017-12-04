package edu.uta.cse6331

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.math._

object Source {
    def main(args: Array[ String ]) {
            val conf = new SparkConf().setAppName("Source")
            val sc = new SparkContext(conf)

            val init = sc.textFile(args(0)).map( line => {
                                val a = line.split(",")
                                (a(2).toInt,  (a(0).toInt,a(1).toInt)) } )
                                                        .groupByKey
                                                        .mapValues(_.toList)
                                                        .map{case(x, y) => (x, (10000000, y))}

            val dis = Array.fill[Long](100000)(10000000)
            dis(0) = 0

            val distanceRDD1 = sc.parallelize(dis.zipWithIndex.map{case (k,v) => (v,k)})
            val distanceBC1 = sc.broadcast(distanceRDD1.collect().toMap)
            val res1 = init.map{case(x, (d, y)) => 
                                    (x, (math.min(d, y.map(n => distanceBC1.value(n._1) + n._2).min), y))}
            val res1_ = res1.map{case(x, (d, y)) => (x, d)}
                


            val distanceRDD2 = (res1_ union distanceRDD1).reduceByKey((a, b) => if (a < b) a else b)
            val distanceBC2 = sc.broadcast(distanceRDD2.collect().toMap)
            val res2 = res1.map{case(x, (d, y)) => 
                                    (x, (math.min(d, y.map(n => distanceBC2.value(n._1) + n._2).min), y))}
            val res2_ = res2.map{case(x, (d, y)) => (x, d)}



            val distanceRDD3 = (res2_ union distanceRDD2).reduceByKey((a, b) => if (a < b) a else b)        
            val distanceBC3 = sc.broadcast(distanceRDD3.collect().toMap)
            val res3 = res2.map{case(x, (d, y)) => 
                                    (x, (math.min(d, y.map(n => distanceBC3.value(n._1) + n._2).min), y))}
            val res3_ = res3.map{case(x, (d, y)) => (x, d)}

            
            val distanceRDD4 = (res3_ union distanceRDD3).reduceByKey((a, b) => if (a < b) a else b)        
            val distanceBC4 = sc.broadcast(distanceRDD4.collect().toMap)
            val res4 = res3.map{case(x, (d, y)) => 
                                    (x, (math.min(d, y.map(n => distanceBC4.value(n._1) + n._2).min), y))}
            val res4_ = res4.map{case(x, (d, y)) => (x, d)}

            val result = (res4_ union distanceRDD4).reduceByKey((a, b) => if (a < b) a else b).filter{case (k, v) => v < 10000000}.sortByKey()
            result.coalesce(1)saveAsTextFile(args(1))
    sc.stop()
  }
}
