package uk.gov.ons.dsc.trade

import org.apache.spark.sql.Row

object ShipProgress1 {

  def compute(t0:Int, t1:Int, ship:Int, filtered: Array[Row]) :Double = {
    // returns the ship progress ratio, i.e. real vs straight line
    if (filtered.length >= 2)
      {
        val sorted = filtered.sortBy(r=>r.getInt(0))

        println ("Number of records in filtered:" + sorted.size)

        val firstRow = sorted.head
        val lastRow  = sorted.last

        val A_lat = firstRow(1).asInstanceOf[Double]
        val A_long = firstRow(2).asInstanceOf[Double]
        val B_lat = lastRow(1).asInstanceOf[Double]
        val B_long = lastRow(2).asInstanceOf[Double]

        val lineDist = Utils.pointDistance((A_lat,A_long),(B_lat,B_long))
        println("Line distanse is:" + lineDist)

        val routePoints =  sorted.map(r=>(r.getDouble(1),r.getDouble(2))).toList
        val routeDist:Double = Utils.trackDistance(routePoints)
        println("Route distanse is:" + routeDist)


        if (routeDist == 0 && lineDist == 0) {
          1.0
        } else if (routeDist > 0 ) {
          val progrRatio =  lineDist/routeDist
          // println("Calc ratio: " + progrRatio)
          progrRatio

        } else {
          println("Error!  Something went wrong in calculation of progress ration (routeDist negative or division by zero)")
          0.0
        }

      } else {
     1.0
    }



  }


}
