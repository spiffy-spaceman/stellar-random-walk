package au.csiro.data61.randomwalk.algorithm

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}


/**
  *
  */

object GraphMap {
  private lazy val srcVertexMap: mutable.Map[Long, Int] = new HashMap[Long, Int]()

  private lazy val offsets: ArrayBuffer[Int] = new ArrayBuffer()
  private lazy val lengths: ArrayBuffer[Int] = new ArrayBuffer()
  private lazy val edges: ArrayBuffer[(Long, Float)] = new ArrayBuffer() // dst vertex with weight

  private var indexCounter: Int = 0
  private var offsetCounter: Int = 0

  private var firstGet: Boolean = true

  private lazy val vertexPartitionMap: mutable.Map[Long, Int] = new HashMap[Long, Int]()

  def addVertex(vId: Long, neighbors: Array[(Long, Int, Float)]): Unit = synchronized {
    srcVertexMap.get(vId) match {
      case None => {
        if (!neighbors.isEmpty) {
          updateIndices(vId, neighbors.length)
          for ((dst, pId, weight) <- neighbors) {
            edges.insert(offsetCounter, (dst, weight))
            offsetCounter += 1
            vertexPartitionMap.put(dst, pId)
          }
        } else {
          this.addVertex(vId)
        }
      }
      case Some(value) => value
    }
  }

  def addVertex(vId: Long, neighbors: Array[(Long, Float)]): Unit = synchronized {
    srcVertexMap.get(vId) match {
      case None => {
        if (!neighbors.isEmpty) {
          updateIndices(vId, neighbors.length)
          for (e <- neighbors) {
            edges.insert(offsetCounter, e)
            offsetCounter += 1
          }
        } else {
          this.addVertex(vId)
        }
      }
      case Some(value) => value
    }
  }

  private def updateIndices(vId: Long, outDegree: Int): Unit =
  {
    srcVertexMap.put(vId, indexCounter)
    offsets.insert(indexCounter, offsetCounter)
    lengths.insert(indexCounter, outDegree)
    indexCounter += 1
  }

  def getPartition(vId: Long): Option[Int] = {
    vertexPartitionMap.get(vId)
  }

  def getGraphStatsOnlyOnce: (Int, Int) = synchronized {
    if (firstGet) {
      firstGet = false
      (srcVertexMap.size, offsetCounter)
    }
    else
      (0,0)
  }

  def resetGetters {
    firstGet = true
  }

  def addVertex(vId: Long): Unit = synchronized {
    srcVertexMap.put(vId, -1)
  }

  def getNumVertices: Int = {
    srcVertexMap.size
  }

  def getNumEdges: Int = {
    offsetCounter
  }

  /**
    * The reset is mainly for the unit test purpose. It does not reset the size of data
    * structures that are initially set by calling setUp function.
    */
  def reset {
    indexCounter = 0
    offsetCounter = 0
    srcVertexMap.clear()
    offsets.clear()
    lengths.clear()
    edges.clear()
    vertexPartitionMap.clear
  }

  def getNeighbors(vid: Long): Array[(Long, Float)] = {
    srcVertexMap.get(vid) match {
      case Some(index) =>
        if (index == -1) {
          return Array.empty[(Long, Float)]
        }
        val offset = offsets(index)
        val length = lengths(index)
        edges.slice(offset, offset + length).toArray
      case None => null
    }
  }
}
