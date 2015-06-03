import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap
import scala.collection.mutable.Set

/*
maxFlow function

Arguments: sourceId (type: VertexId)
           targetId (type: VertexId)
           graph (type: Graph[Long,Int])
Returns: the maximum flows as RDD[(VertexId,VertexId),Int]

Note that the maxFlow function is defined below the shortestPath function
 */

def shortestPath(sourceId: VertexId, targetId: VertexId, graph: Graph[Long, Int]): (Set[(VertexId, VertexId)], Int) = {
  val test: VertexId = sourceId // default vertex id

  // Initialize the graph such that all vertices except the root have distance infinity.
  // Vertices will have attributes (distance,capacity,id)
  // Note that Int.MaxValue - 1 is used to symbolise infinity

  // initializing the vertex attributes
  val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) (0, Int.MaxValue - 1, id)
  else (Int.MaxValue - 1, Int.MaxValue - 1, id))

  val sssp = initialGraph.pregel((Int.MaxValue - 1, Int.MaxValue - 1, test))(

    // Vertex program
    (id, dist, newDist) => {
      if (dist._1 < newDist._1) dist
      else newDist
    },

    // send message
    triplet => {
      // If distance of source + 1 is less than attribute of destination => update
      // If capacity along edges <= 0; do not propagate message
      if (triplet.srcAttr._1 + 1 < triplet.dstAttr._1 && triplet.attr > 0) {
        Iterator((triplet.dstId, (triplet.srcAttr._1 + 1, math.min(triplet.srcAttr._2, triplet.attr), triplet.srcId)))
      }
      else {
        Iterator.empty
      }
    },

    // merge multiple messages
    (a, b) => {
      if (a._1 < b._1) a // if distance from a is less than distance from b 
      else if ((a._1 == b._1) && (a._2 > b._2)) a // if they're equal but a has a higher minimum capacity seen so far
      else b
    }
  )

  val vNum = sssp.vertices.count.toInt // Number of elements < n
  val v = sssp.vertices.take(vNum) // Convert RDD to array
  val minCap = sssp.vertices.filter(v => v._1 == targetId).first._2._2
  val path = Set[(VertexId, VertexId)]()


  // Check if there is a path or not; if no path => return empty set
  if (minCap != (Int.MaxValue - 1)) {
    val links = new HashMap[VertexId, VertexId]
    for (i <- 0 to vNum - 1) {
      links += v(i)._1 -> v(i)._2._3
    }

    // Build the set of edges in the shortest path
    var id = targetId
    for (i <- 0 to vNum - 1; if id != sourceId) {
      path += ((links(id), id))
      id = links(id)
    }
  }

  return (path, minCap)
}


def maxFlow ( sourceId: VertexId, targetId: VertexId, graph: Graph[Long,Int] ) : RDD[((VertexId,VertexId),Int)] = {

  val edges = graph.edges
  val vertices = graph.vertices
  var flows: RDD[((VertexId, VertexId), Int)] = edges.map(e => ((e.srcId, e.dstId), 0))
  var residual: Graph[Long, Int] = graph // Initially zero flow => residual = graph

  var shortest = shortestPath(sourceId, targetId, residual)
  var path = shortest._1
  var minCap = shortest._2
  val empty = Set[(VertexId, VertexId)]() // Empty set

  // Note that algorithm only terminates when there are no more shortest paths
  // This could potentially be a long time, so the code user should edit in a max number
  // of iterations here if needed
  while (path != empty) {
    val bcPath = sc.broadcast(path)

    // Update the flows
    val updateFlow = minCap
    val newFlows = flows.map(e => {
      if (bcPath.value contains e._1) {
        (e._1, e._2 + updateFlow)
      }
      else {
        e
      }
    })

    flows = newFlows

    // Update the residual graph
    val newEdges = residual.edges.map(e => ((e.srcId, e.dstId), e.attr)).
      flatMap(e => {
      if (bcPath.value contains e._1) {
        Seq((e._1, e._2 - minCap), ((e._1._2, e._1._1), minCap))
      }
      else Seq(e)
    }
      ).reduceByKey(_ + _)

    residual = Graph(vertices, newEdges.map(e => Edge(e._1._1, e._1._2, e._2)))

    // Compute for next iteration
    shortest = shortestPath(sourceId, targetId, residual)
    path = shortest._1
    minCap = shortest._2
  }

  return flows
}
