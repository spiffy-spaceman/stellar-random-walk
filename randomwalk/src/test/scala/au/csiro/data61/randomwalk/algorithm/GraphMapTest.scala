package au.csiro.data61.randomwalk.algorithm

import org.scalatest.FunSuite

class GraphMapTest extends FunSuite {

  test("test GraphMap data structure") {
    val e1 = Array((2L, 1.0f))
    val e2 = Array((3L, 1.0f))
    val e3 = Array((3L, 1.0f))
    val e4 = Array((1L, 1.0f))
    var v2N = Array((1L, e1))
    GraphMap.addVertex(1L, e1)
    GraphMap.addVertex(2L)
    assert(GraphMap.getNumEdges == 1)
    assert(GraphMap.getNumVertices == 2)
    assertMap(v2N, GraphMap)

    GraphMap.reset
    v2N = Array((1L, e1 ++ e2))
    GraphMap.addVertex(1L, e1 ++ e2)
    GraphMap.addVertex(2L)
    GraphMap.addVertex(3L)
    assertMap(v2N, GraphMap)


    GraphMap.reset
    v2N = v2N ++ Array((2L, e3 ++ e4))
    GraphMap.addVertex(2L, e3 ++ e4)
    GraphMap.addVertex(1L, e1 ++ e2)
    GraphMap.addVertex(3L)
    assertMap(v2N, GraphMap)
  }

  private def assertMap(verticesToNeighbors: Array[(Long, Array[(Long,Float)])], gMap: GraphMap.type) = {
    for (v <- verticesToNeighbors) {
      var neighbors: Array[(Long, Float)] = Array()
      for (e <- v._2) {
        neighbors = neighbors ++ Array((e._1, e._2))
      }
      assert(gMap.getNeighbors(v._1) sameElements neighbors)
    }
  }

}
