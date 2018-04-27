package com.cs6240.spark.pgrank

import java.io.FileWriter;

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._;
import scala.collection.mutable._;
import org.apache.spark.storage._;

import java.util.regex.Pattern;
import java.util.regex.Matcher;


object PageRank {
  
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("PageRank").setMaster("local")
    val sc = new SparkContext(conf)

    // Code to read bz2 and generate graph line by line
    val input = sc.textFile(args(0));

    val graph = input.filter(l=>l.split(":")(0).matches("^([^~]+)$")).mapPartitions(x => {
      val p = new Parser()
      x.map(x => (x.split(":")(0),p.makeGraph(x).asScala))
    }, true).cache()
    
    // To handle Dangling nodes
    val pgraph=graph.values.flatMap(value => value).map(line => (line, Buffer[String]())).union(graph).reduceByKey((x,y) => (x++y))
    
    val v = pgraph.count
    
    // Initialize the graph to pagerank values
    val rank = 1.0/v
    var pageRankGraph = pgraph.map( node => (node._1, rank))
    val alpha = 0.15
   
    val initial_graph = pgraph
    
    val accum = sc.doubleAccumulator
    
    //Run 10 iterations of pagerank
    for (i <- 1 to 10) {
      var oldDelta = accum.value;
      accum.reset()
      
      // For each node in graph, if no adj list then no contribution and add to accum else send contribution. Then, reduce by key
      var adj = pageRankGraph.join(initial_graph).map(each => (each._1,(each._2._1+((1-alpha)*oldDelta)/v,each._2._2))).values.flatMap{line => {
        var buffer_size = line._2.size
        if(buffer_size ==0) {
          accum.add(line._1)
          line._2.map(l => (l,0.0))
          }
        else {line._2.map(l => (l,line._1/buffer_size.toDouble))}
         }
      }.reduceByKey(_+_)
      adj.count
      
      // To get dangling nodes which are not in adj
      var remains = pageRankGraph.subtractByKey(adj)
      
      var finalRank=adj.mapValues(p => (((1-alpha)*p)+(alpha/v)))
      
      // Join finalRank and remains to get the graph for next iteration
      pageRankGraph = (finalRank).union(remains)
		  if(i==10) {
			  val f =finalRank.sortBy(_._2,false).take(100)
			  val topk = sc.parallelize(f,1).saveAsTextFile(args(1))
			  }
      
    }
    
  }
}