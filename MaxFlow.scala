import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap // Need this for HashMap
//import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Set
import scala.util.control._ // Need this to be able to do break

// Import vertices
val vertices = sc.textFile("data/toy-vertices.txt").
                flatMap(line => line.split(" ")).
                map(l => (l.toLong,"vertex")) // Vertex needs a property and needs to be type long

// Import Edges
val edges = sc.textFile("data/toy-edges.txt").
                map(line => line.split(" ")).
                map(e => Edge(e(0).toLong, e(1).toLong, e(2).toLong))

// Build RDD of flows
val flows = edges.map(e => ( (e.srcId,e.dstId), 0.0) )

// Create Graph (and residual graph)
val residual = Graph(vertices, edges)

// Shortest Path, TODO MAKE THIS INTO A METHOD
val sourceId: VertexId = 1 // The source
val targetId: VertexId = 4 // The target
val test: VertexId = 5 // default vertex id, probably need to change

// Initialize the graph such that all vertices except the root have distance infinity.
// Note that now vertices will be of type [VertexId, (distance,mincap,id)]
// Shouldn't distance and mincap be integer? We only allow integer capacities? No need for double

val initialGraph = residual.mapVertices( (id, _) => if (id == sourceId) (0.0, Double.PositiveInfinity, id)
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
    // if capacity along edges == 0; do not propagate message
    if (triplet.srcAttr._1 + 1 < triplet.dstAttr._1 && triplet.attr > 0) {
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

/* TODO : 

		  stop pregel sp when target node is hit --> pregel API does not seem to offer the possibility
		  maybe add an extra case in the sendMsg and send an empty iterator if target node is hit?
*/

// What happens here if nodes are unvisited? We do not want those in our set. Add if != INF statement?
val vNum = sssp.vertices.count.toInt // Number of elements < n
val v = sssp.vertices.take(vNum) // Convert RDD to array
val minCap = sssp.vertices.filter(v => v._1 == targetId).first._2._2

val links = new HashMap[VertexId, VertexId]
for ( i <- 0 to vNum-1 ) {
  links += v(i)._1 -> v(i)._2._3
}

// Build the set of edges in the shortest path

val path = Set[(VertexId, VertexId)]() 
var id = targetId
val loop = new Breaks // Needed to do break
for ( i <- 0 to vNum-1 ) {
  if (id == sourceId) {
    loop.break
  }
  path += ((links(id),id))
	id = links(id)
}

val bcPath = sc.broadcast(path)

// Update the flows RDD
// TODO: update flows and not otherflows

val otherflows = flows.flatmap(e => 
{ 
	if (bcPath.value contains e._1)
	{
		(e._1, e._2 + minCap)
	}
	else
	{
		e
	}													 
})
/*
// Update the residual graph
val newEdges = residual.edges.map(

)
*/
println("Shortest Path is: ")
println(path)
