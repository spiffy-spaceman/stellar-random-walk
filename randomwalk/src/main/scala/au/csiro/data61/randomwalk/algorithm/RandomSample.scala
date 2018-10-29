package au.csiro.data61.randomwalk.algorithm

import scala.util.Random

/* TODO: Use Alias sampling */
case class RandomSample(nextFloat: () => Float = Random.nextFloat) extends Serializable {


  /**
    *
    * @return
    */
  final def sample(edges: Array[(Long, Float)]): (Long, Float) = {

    val sum = edges.foldLeft(0.0) { case (w1, (_, w2)) => w1 + w2 }

    val p = nextFloat()
    var acc = 0.0
    for ((dstId, w) <- edges) {
      acc += w / sum
      if (acc >= p)
        return (dstId, w)
    }

    edges.head
  }

  final def computeSecondOrderWeights(p: Float = 1.0f,
                                      q: Float = 1.0f,
                                      prevId: Long,
                                      prevNeighbors: Array[(Long, Float)],
                                      currNeighbors: Array[(Long, Float)]): Array[(Long, Float)
    ] = {
    currNeighbors.map { case (dstId, w) =>
      var unnormProb = w / q // Default is that there is no direct link between src and
      // dstNeighbor.
      if (dstId == prevId) unnormProb = w / p // If the dstNeighbor is the src node.
      else {
        if (prevNeighbors.exists(_._1 == dstId)) unnormProb = w
      }
      (dstId, unnormProb)
    } // If there is a
    // direct link from src to neighborDst. Note, that the weight of the direct link is always
    // considered, which does not necessarily is the shortest path.
  }

  /**
    *
    * @param p
    * @param q
    * @param prevId
    * @param prevNeighbors
    * @param currNeighbors
    * @return
    */
  final def secondOrderSample(p: Float = 1.0f,
                              q: Float = 1.0f,
                              prevId: Long,
                              prevNeighbors: Array[(Long, Float)],
                              currNeighbors: Array[(Long, Float)]): (Long, Float) = {
    val newCurrentNeighbors = computeSecondOrderWeights(p, q, prevId, prevNeighbors, currNeighbors)
    sample(newCurrentNeighbors)
  }
}
