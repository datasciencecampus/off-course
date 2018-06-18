package uk.gov.ons.dsc.trade

object Utils {

  def pointDistance(pointA: (Double, Double), pointB: (Double, Double)): Double = {
    val deltaLat = math.toRadians(pointB._1 - pointA._1)
    val deltaLong = math.toRadians(pointB._2 - pointA._2)
    val a = math.pow(math.sin(deltaLat / 2), 2) + math.cos(math.toRadians(pointA._1)) * math.cos(math.toRadians(pointB._1)) * math.pow(math.sin(deltaLong / 2), 2)
    val greatCircleDistance = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    3958.761 * greatCircleDistance
  }

  def trackDistance (points: List[(Double, Double)]):Double = {
    // calculates the disntance of the track as sum of the distances between the point
    points match {
      case head :: tail => tail.foldLeft(head, 0.0)((accum, elem) => (elem, accum._2 + pointDistance(accum._1, elem)))._2
      case Nil => 0.0
    }

  }
  def parseDouble (s:String) = try {Some(s.toDouble)} catch { case _: Throwable => None}
  def parseInt (s:String) = try {Some(s.toInt)} catch { case _: Throwable => None}
}
