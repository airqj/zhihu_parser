package computeSimilar

class computeSimilar {
//  private[computeSimilar] val weight = (0.5,0.3,0.2)
  private[computeSimilar] val weight = (1,1,1)
  private def singleDimensional(a: List[Long],b: List[Long]): Double = {
    val aSet = a.toSet
    val bSet = b.toSet
//    val res = aSet.intersect(bSet).size.toDouble / aSet.union(bSet).size
    val res = aSet.intersect(bSet).size.toDouble
    res
  }
  def jaccardSimilarity(user1: (List[Long],List[Long],List[Long]),user2: (List[Long],List[Long],List[Long])):Double = {
    val jaccardRes = (singleDimensional(user1._1,user2._1),singleDimensional(user1._2,user2._2),singleDimensional(user1._3,user2._3))
    val similarity = jaccardRes._1*weight._1 + jaccardRes._2 * weight._2 + jaccardRes._3 * weight._3
    similarity
  }
}
