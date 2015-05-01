import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

// TEST SHORTEST PATH

// Import vertices
val vertices = sc.textFile("data/toy-vertices.txt").
  flatMap(line => line.split(" ")).
  map(l => (l.toLong,"vertex")) // Vertex needs a property and needs to be type long

println("Vertices:")
vertices.foreach(v => println(v))

// Import Edges
val edges = sc.textFile("data/toy-edges.txt").
  map(line => line.split(" ")).
  map(e => Edge(e(0).toLong, e(1).toLong, e(2).toLong))

println("Edges:")
edges.foreach(e => println(e))

// Create Graph
val graph = Graph(vertices, edges)

// Calculate inDegrees for fun
val inDegrees = graph.inDegrees
inDegrees.foreach(d => println(d))

// Shortest Path
val sourceId: VertexId = 1 // The source
val targetId: VertexId = 4 // The target
val test: VertexId = 5 // default vertex id, probably need to change
// Initialize the graph such that all vertices except the root have distance infinity.
// Note that now vertices will be of type [VertexId, (distance,id)]
val initialGraph = graph.mapVertices( (id, _) => if (id == sourceId) (0.0, id) else (Double.PositiveInfinity, id))

val sssp = initialGraph.pregel((Double.PositiveInfinity, test))(
  (id, dist, newDist) => {
    if (dist._1 < newDist._1) dist // dist(0) = first entry of (distance,id)
    else newDist
  },
  triplet => { // srcAttr, attribute of where message comes from
    // dstAttr, attribute of destination
    // attr of edge
    // So if distance of source + weight of edge connecting to destination is
    // less than attribute of destination => update
    if (triplet.srcAttr._1 + triplet.attr < triplet.dstAttr._1) {
      Iterator((triplet.dstId, (triplet.srcAttr._1 + triplet.attr, triplet.srcId)))
      // Now you change the new vertex to be [id of destination, (distance, sourceId)
    }
    else {
      Iterator.empty
    }
  },
  (a, b) => {
    if (a._1 < b._1) a
    else b
  }
)

sssp.vertices.collect()

/*
val ans: RDD[Int] = sssp.vertices.map(vertex =>
  "Vertex " + vertex._1 + ": distance is " + vertex._2(0) + ", previous node is Vertex " + vertex._2(1).toInt)


ans.take(10).mkString("\n") */