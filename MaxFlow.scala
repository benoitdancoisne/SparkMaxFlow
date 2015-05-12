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
                map(l => (l.toLong, None)) // Vertex needs a property

// Import Edges
val edges = sc.textFile("data/toy-edges.txt").
                map(line => line.split(" ")).
                map(e => Edge(e(0).toLong, e(1).toLong, e(2).toInt))

// Build RDD of flows (Needs to be var as it is going to be updated)
var flows: RDD[((VertexId,VertexId),Int)] = edges.map(e => ( (e.srcId,e.dstId), 0) )

// Create Graph Residual graph (needs to be var)
var residual: Graph[None.type,Int] = Graph(vertices, edges)


// FOR LOOP, WE WANT TO CLEAN THIS SHIZ UP BAD
// Which upper bound should we put? How many iterations in for loop?
// Note that break in scala breaks out of ALL for loops, not just one

// Shortest Path, TODO MAKE THIS INTO A METHOD
val sourceId: VertexId = 1 // The source
val targetId: VertexId = 4 // The target
val test: VertexId = 5 // default vertex id, probably need to change

// Initialize the graph such that all vertices except the root have distance infinity.
// Note that now vertices will be of type [VertexId, (distance,mincap,id)]
// Note that the equivalent of Double.PositiveInfinity  is Int.MaxValue. But we check
// srcAttr._1 + 1, so we need to make sure there is no overflow, so set it to Int.MaxValue - 1
// Otherwise Double.PositiveInfinity.toInt?

val initialGraph = residual.mapVertices((id, _) => if (id == sourceId) (0, Int.MaxValue - 1, id)
else (Int.MaxValue - 1, Int.MaxValue - 1, id))


val sssp = initialGraph.pregel((Int.MaxValue - 1, Int.MaxValue - 1, test))(

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
      Iterator((triplet.dstId, (triplet.srcAttr._1 + 1, math.min(triplet.srcAttr._2, triplet.attr), triplet.srcId)))
    }
    else {
      Iterator.empty
    }
  },

  // mergeMsg
  (a, b) => {
    if (a._1 < b._1) a
    else if ((a._1 == b._1) && (a._2 > b._2)) a
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
for (i <- 0 to vNum - 1) {
  links += v(i)._1 -> v(i)._2._3
}

// Build the set of edges in the shortest path

val path = Set[(VertexId, VertexId)]()
var id = targetId
val loop = new Breaks // Needed to do break
for (i <- 0 to vNum - 1) {
  if (id == sourceId) {
    loop.break
  }
  path += ((links(id), id))
  id = links(id)
}

val bcPath = sc.broadcast(path)

// Update the flows RDD
// TODO: update flows and not otherflows

val newFlows = flows.map(e => {
  if (bcPath.value contains e._1) {
    (e._1, e._2 + minCap)
  }
  else {
    e
  }
})

flows = newFlows

// Update the residual graph

val newEdges = residual.edges.map(e => ((e.srcId, e.dstId), e.attr)).
                              flatMap(e =>
                              {
                                if (bcPath.value contains e._1) {
                                  Seq((e._1, e._2 - minCap), ((e._1._2, e._1._1), minCap))
                                }
                                else Seq(e)
                              }
                              ).reduceByKey(_ + _)

residual = Graph(vertices, newEdges.map(e => Edge(e._1._1, e._1._2, e._2) ) )

println("Shortest Path is: ")
println(path)
