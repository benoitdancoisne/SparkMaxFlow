import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

// Import vertices
val vertices = sc.textFile("../../SparkMaxFlow/data/toy-vertices.txt").
                flatMap(line => line.split(" ")).
                map(l => (l.toLong,"vertex")) // Vertex needs a property and needs to be type long

println("Vertices:")
vertices.foreach(v => println(v))

// Import Edges
val edges = sc.textFile("../../SparkMaxFlow/data/toy-edges.txt").
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
// Initialize the graph such that all vertices except the root have distance infinity.
val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
val sssp = initialGraph.pregel(Double.PositiveInfinity)(
  (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
  triplet => {  // Send Message
    if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
      Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
    } else {
      Iterator.empty
    }
  },
  (a,b) => math.min(a,b) // Merge Message
)
println(sssp.vertices.collect.mkString("\n"))