package uk.gov.ons.dsc.trade

import org.apache.spark.sql.DataFrame

object ShipProgress {

  def compute(t0:Int, t1:Int, ship:Int, df: DataFrame) :Double = {
    // returns the ship progress ration, i.e. real vs straight line
    val filtered = df.filter(df.col("MMSI") === ship && df.col("time") >= t0 && df.col("time") <= t1 )
                     .sort("time")
            //         .dropDuplicates(Seq("time","MMSI","long","lat"))
           //          .cache()

    println ("Number of records in filtered:" + filtered.count())

    val firstRow = filtered.head()
    println("#1")
    val lastRow  = filtered.reduce {
                       (a,b) => if(a.getAs[Int]("time")  > b.getAs[Int]("time")) a else b
                       }
    println("#1")

    val A_lat = firstRow.getAs[Double] ("lat")
    val A_long = firstRow.getAs[Double] ("long")
    val B_lat = lastRow.getAs[Double] ("lat")
    val B_long = lastRow.getAs[Double] ("long")

    val lineDist = Utils.pointDistance((A_lat,A_long),(B_lat,B_long))
    println("Line distanse is:" + lineDist)

    val routePoints =  filtered.select(filtered.col("lat"), filtered.col("long")).rdd.map(r=>(r.getDouble(0),r.getDouble(1))).collect.toList
    val routeDist:Double = Utils.trackDistance(routePoints)

    //filtered.unpersist()
    if (routeDist > 0) {
      val progrRatio =  lineDist/routeDist
      println("Calc ratio: " + progrRatio)
      progrRatio

    } else {
      println("Error!  Something went wrong in calculation of progress ration (routeDist negative or division by zero)")
       0.0
    }


  }


}
