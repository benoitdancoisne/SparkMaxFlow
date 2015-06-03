import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap
import scala.collection.mutable.Set
import org.apache.spark.graphx.util.GraphGenerators

/*
Example use of maxFlow function
(note that maxFlow function needs to be imported in order to run this)
 */

// Running maxFlow on a few random graphs
for (i <- 1 to 3 )
{
  var n: Int = 10*i // Number of vertices

  // Create random graph with random source and target
  var graph = GraphGenerators.logNormalGraph(sc, n, 5)
  val r = scala.util.Random
  val sourceId: VertexId = r.nextInt(n) // The source
  val targetId: VertexId = r.nextInt(n) // The target

  // Calculate Max Flow
  val t0 = System.nanoTime()
  val flows = maxFlow(sourceId, targetId, graph)
  val t1 = System.nanoTime()

  // Print information
  val emanating = flows.filter(e => e._1._1 == sourceId).map(e => (e._1._1,e._2)).reduceByKey(_ + _).collect
  println("Number of Edges: ")
  println(flows.count)
  println("Max Flow: ")
  println(emanating(0)._2)
  println("Time: ")
  println((t1 - t0)/1e9)
}



/*

//To use a graph imported from a txt file, uncomment this section

// Import vertices
val vertices = sc.textFile("data/toy-vertices-big.txt").
  flatMap(line => line.split(" ")).
  map(l => (l.toLong, 0.toLong)) // Vertex needs a property

// Import Edges
val edges = sc.textFile("data/toy-edges-big.txt").
  map(line => line.split(" ")).
  map(e => ((e(0).toLong, e(1).toLong), e(2).toInt)).
  reduceByKey(_ + _).
  map(e => Edge(e._1._1, e._1._2, e._2))

// Create Graph
val g: Graph[Long,Int] = Graph(vertices, edges)

val s: VertexId = 42 // The source
val t: VertexId = 73 // The target

val t0 = System.nanoTime()
val flows = maxFlow(s, t, g)
val t1 = System.nanoTime()

println("Time: ")
println((t1 - t0)/1e9)

val emanating = flows.filter(e => e._1._1 == s).map(e => (e._1._1,e._2)).reduceByKey(_ + _).collect
println("Max Flow: ")
println(emanating(0)._2)

*/