package edu.uta.cse6331

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.math._

object Source {
    def main(args: Array[ String ]) {
            val conf = new SparkConf().setAppName("Source")
            val sc = new SparkContext(conf)
            var distance = Array.fill[Long](1000)(10000000)
            distance(0) = 0
            val init = sc.textFile(args(0)).map( line => {
                                            val a = line.split(",")
                                            (a(2).toInt,  (a(0).toInt,a(1).toInt)) } )
                                                                    .groupByKey
                                                                    .mapValues(_.toList)
                                                                    .map{case(x, y) => (x, distance(x), y)}


            val res1 = init.map{case(x, d, y) => 
            						(x, math.min(d, y.map(n => distance(n._1) + n._2).min), y)
            					}
            res1.map{case(x, d, _) =>
            			distance(x) = d
            		}
            val res2 = res1.map{case(x, d, y) => 
						(x, math.min(d, y.map(n => distance(n._1) + n._2).min), y)
					}
	        res2.map{case(x, d, _) =>
    			distance(x) = d
    		}
            val res3 = res2.map{case(x, d, y) => 
						(x, math.min(d, y.map(n => distance(n._1) + n._2).min), y)
					}

			res3.map{case(x, d, _) =>
    			distance(x) = d
    		}
            val res4 = res3.map{case(x, d, y) => 
						(x, math.min(d, y.map(n => distance(n._1) + n._2).min))
					}

			res4.map{case(x, d) =>
    			distance(x) = d
    		}

		/*	println(distance(0))
			println(distance(1))
			println(distance(2))
			println(distance(3))
			println(distance(4))	*/		

                                                
           // res4.collect().foreach(println)   

          //  for (i <- 1 to 4) {


          //  }



        res4.coalesce(1)saveAsTextFile(args(1))
    sc.stop()
  }
}
