import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap // Need this for HashMap
import scala.collection.mutable.ListBuffer
import scala.util.control._ // Need this to be able to do break

// TEST SHORTEST PATH

// Import vertices
val vertices = sc.textFile("data/toy-vertices.txt").
  flatMap(line => line.split(" ")).
  map(l => (l.toLong,"vertex")) // Vertex needs a property and needs to be type long

/*
println("Vertices:")
vertices.foreach(v => println(v)) */

// Import Edges
val edges = sc.textFile("data/toy-edges.txt").
  map(line => line.split(" ")).
  map(e => Edge(e(0).toLong, e(1).toLong, e(2).toLong))

/*
println("Edges:")
edges.foreach(e => println(e)) */

// Create Graph
val graph = Graph(vertices, edges)

// Shortest Path
val sourceId: VertexId = 1 // The source
val targetId: VertexId = 4 // The target
val test: VertexId = 5 // default vertex id, probably need to change


// Initialize the graph such that all vertices except the root have distance infinity.
// Note that now vertices will be of type [VertexId, (distance,mincap,id)]

val initialGraph = graph.mapVertices( (id, _) => if (id == sourceId) (0.0, Double.PositiveInfinity, id)
																								 else (Double.PositiveInfinity, Double.PositiveInfinity, id))


val sssp = initialGraph.pregel((Double.PositiveInfinity, Double.PositiveInfinity, test))(
  
	// Vprog
	(id, dist, newDist) => {
    if (dist._1 < newDist._1) dist
    else newDist
  },

	// sendMsg
  triplet => {
    // So if distance of source + weight of edge connecting to destination is
    // less than attribute of destination => update
    if (triplet.srcAttr._1 + 1 < triplet.dstAttr._1) {
      Iterator((triplet.dstId, (triplet.srcAttr._1 + 1, math.min(triplet.srcAttr._2,triplet.attr),triplet.srcId)))
    }
    else {
      Iterator.empty
    }
  },

	// mergeMsg
  (a, b) => {
    if (a._1 < b._1) a
    else if ((a._1 == b._1)&&(a._2 > b._2)) a
		else b
  }
)

sssp.vertices.collect()

// TODO : recover shortest path + min capaacity
// What happens here if nodes are unvisited? We do not want those in our set. Add if != INF statement?
val vNum = sssp.vertices.count.toInt // Number of elements < n
val v = sssp.vertices.take(vNum) // Convert RDD to array
val minCap = sssp.vertices.filter(v => v._1 == targetId).first._2._2

val links = new HashMap[VertexId, VertexId]
for ( i <- 0 to vNum-1 ) {
  links += v(i)._1 -> v(i)._2._3
}

/* This would be more elegant but does not work
sssp.vertices.foreach( v => links += v._1 -> v._2._2 )
*/

println(links)

/*
// Determine the path
// Create a list of edges first and then convert to RDD
val path = ListBuffer[VertexId](targetId) // Use ListBuffer instead of List as it is mutable
val loop = new Breaks // Needed to do break
for ( i <- 0 to vNum-1 ) {
  val id = path.head // First element of ListBuffer
  if (id == sourceId) {
    loop.break
  }
  path.prepend(links(id))
} */

// Store path as edges
val path = ListBuffer[Edge[Double]](Edge(links(targetId),targetId,minCap))
val loop = new Breaks // Needed to do break
for ( i <- 0 to vNum-1 ) {
  val id = path.head.srcId // First element of ListBuffer
  if (id == sourceId) {
    loop.break
  }
  path.prepend(Edge(links(id),id,minCap))
}

val edgePath = sc.parallelize(path)

println("Shortest Path is: ")
println(path)
////////////////////////////////////////////////////////////////////////////////////////////////

/* OLD SHORTEST PATH
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
*/



