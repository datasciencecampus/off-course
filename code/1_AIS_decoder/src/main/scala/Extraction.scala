package uk.gov.ons.dsc.trade

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/*This object extracts statisticcs like shipsPerDay, ShipsPerHour for tiles within a rectangular given by two points,i.e. lat/lon of top left and lat/lon of botom right corners*/

object Extraction {





  def main(args: Array[String]): Unit = {

    if (args.length != 8) {println("wrong parameters! Usage: latA lonA latB longB timeFrom timeTo segmentsLat segmentsLong");return}

    val spark = SparkSession
      .builder()
      .master("yarn")
      //  .deploy-mode("client")
      .appName("AIS-Extraction")
      .config("spark.dynamicAllocation.enabled","false")
      .config("spark.executor.memory","35g")
      .config("spark.executor.cores","6")
      .config("spark.executor.instances","20")
      .getOrCreate()


    // parsing the parameters
    val lat1 = Utils.parseDouble(args(0)).getOrElse(51.0)
    val long1 = Utils.parseDouble(args(1)).getOrElse(1.0)

    val lat2 = Utils.parseDouble(args(2)).getOrElse(50.0)
    val long2 = Utils.parseDouble(args(3)).getOrElse(2.0)

    val time1 = Utils.parseInt(args(4)).getOrElse(0)
    val time2 = Utils.parseInt(args(5)).getOrElse(2147483647)

    val LatSegs:Int = Utils.parseInt(args(6)).getOrElse(1)
    val LongSegs:Int = Utils.parseInt(args(7)).getOrElse(1)


    val LatSegment:Double = lat2 - lat1
    val LongSegment:Double = long2 - long1

    val LatInc = if (LatSegs > 0) {
      LatSegment/LatSegs
    } else {
      LatSegment
    }

    val LongInc = if (LongSegs > 0) {
      LongSegment/LongSegs
    } else {
      LongSegment
    }



      val grid:Array[Array[(Double,Double)]] = Array.tabulate [(Double,Double)](LatSegs+1,LongSegs+1) ((x,y)=> (lat1+x*LatInc,long1+y*LongInc))


      val ais = spark.sql("SELECT time , port, ship, long , lat , MMSI, NavStat, ROT, SOG, COG, HDG, TimeStamp FROM parquet.`/tactical_prod1/raw/data_science_campus/ais/AIS_mess_type1`  ")
        .coalesce(120).cache()


      var ShipsPerHour  = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], dfStructSPH)
      var ShipsPerDay  = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], dfStructSPD)
      var ShipsPerTimeOfDay  = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], dfStructSPTD)
      var ShipsPerDayOfWeek  = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], dfStructSPDW)

/////////////////////// ShipsPerHour
    for (i <- 0 to LatSegs-1; j <- 0 to LongSegs-1) {    // iterate through the tiles
       println("Processing cell:("+i+","+j+") with coordinates TL:"+ grid(i)(j) + " BR:" + grid(i+1)(j+1) )
       val ais10 = prepTileData (grid(i)(j),grid(i+1)(j+1),time1,time2,ais)  //.cache()
       println("Number of records found in the cell: " + ais10.count)

       println ("calcShipsPerHour ...")
       ShipsPerHour = ShipsPerHour.union(calcShipsPerHour (grid(i)(j),grid(i+1)(j+1),ais10).withColumn("A_lat",lit (grid(i)(j)._1)).withColumn("A_lon",lit (grid(i)(j)._2)).withColumn("B_lat",lit (grid(i+1)(j+1)._1)).withColumn("B_lon",lit (grid(i+1)(j+1)._2)) )

      }

    println ("Final calcShipsPerHour ..."+ShipsPerHour.count)

    ShipsPerHour
       .repartition(1)
      //.sort("lat","long")
      .write.mode(SaveMode.Overwrite)
      .save("/tactical_prod1/raw/data_science_campus/ais/ShipsPerHour_"+lat1+"_"+long1+"_"+lat2+"_"+long2+"__"+time1+"_"+time2+"_"+LatSegs+"_"+LongSegs )
    //ShipsPerHour.show()
    //ShipsPerHour = null

///////////////////////// ShipsPerDay
    for (i <- 0 to LatSegs-1; j <- 0 to LongSegs-1) {    // iterate through the tiles
      println("Processing cell:("+i+","+j+") with coordinates TL:"+ grid(i)(j) + " BR:" + grid(i+1)(j+1) )
      val ais10 = prepTileData (grid(i)(j),grid(i+1)(j+1),time1,time2,ais)  //.cache()
      println("Number of records found in the cell: " + ais10.count)

      println ("calcShipsPerDay ...")
      ShipsPerDay = ShipsPerDay.union(calcShipsPerDay (grid(i)(j),grid(i+1)(j+1),ais10).withColumn("A_lat",lit (grid(i)(j)._1)).withColumn("A_lon",lit (grid(i)(j)._2)).withColumn("B_lat",lit (grid(i+1)(j+1)._1)).withColumn("B_lon",lit (grid(i+1)(j+1)._2)) )

    }

    println ("Final calcShipsPerDay ..."+ShipsPerDay.count())
    ShipsPerDay
      .coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .save("/tactical_prod1/raw/data_science_campus/ais/ShipsPerDay_"+lat1+"_"+long1+"_"+lat2+"_"+long2+"__"+time1+"_"+time2+"_"+LatSegs+"_"+LongSegs )

     ShipsPerDay = null
    //ShipsPerDay.show()

/////////////////////////  ShipsPerTimeOfDay
    for (i <- 0 to LatSegs-1; j <- 0 to LongSegs-1) {    // iterate through the tiles
      println("Processing cell:("+i+","+j+") with coordinates TL:"+ grid(i)(j) + " BR:" + grid(i+1)(j+1) )
      val ais10 = prepTileData (grid(i)(j),grid(i+1)(j+1),time1,time2,ais)  //.cache()
      println("Number of records found in the cell: " + ais10.count)

      println ("calcShipsPerTimeOfDay ...")
      ShipsPerTimeOfDay = ShipsPerTimeOfDay.union(calcShipsPerTimeOfDay (grid(i)(j),grid(i+1)(j+1),ais10).withColumn("A_lat",lit (grid(i)(j)._1)).withColumn("A_lon",lit (grid(i)(j)._2)).withColumn("B_lat",lit (grid(i+1)(j+1)._1)).withColumn("B_lon",lit (grid(i+1)(j+1)._2)) )
    }

    println ("Final calcShipsPerTimeOfDay ..." + ShipsPerTimeOfDay.count())
    ShipsPerTimeOfDay
      .coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .save("/tactical_prod1/raw/data_science_campus/ais/ShipsPerTimeOfDay_"+lat1+"_"+long1+"_"+lat2+"_"+long2+"__"+time1+"_"+time2+"_"+LatSegs+"_"+LongSegs )

     ShipsPerTimeOfDay = null
    //ShipsPerTimeOfDay.show()

///////////////////////// ShipsPerDayOfWeek
    for (i <- 0 to LatSegs-1; j <- 0 to LongSegs-1) {    // iterate through the tiles
      println("Processing cell:("+i+","+j+") with coordinates TL:"+ grid(i)(j) + " BR:" + grid(i+1)(j+1) )
      val ais10 = prepTileData (grid(i)(j),grid(i+1)(j+1),time1,time2,ais)  //.cache()
      println("Number of records found in the cell: " + ais10.count)

      println ("calcShipsPerDayOfWeek ...")
      ShipsPerDayOfWeek = ShipsPerDayOfWeek.union(calcShipsPerDayOfWeek (grid(i)(j),grid(i+1)(j+1),ais10).withColumn("A_lat",lit (grid(i)(j)._1)).withColumn("A_lon",lit (grid(i)(j)._2)).withColumn("B_lat",lit (grid(i+1)(j+1)._1)).withColumn("B_lon",lit (grid(i+1)(j+1)._2)) )
      println (" ")


    }

    println ("Final calcShipsPerDayOfWeek ..."+ShipsPerDayOfWeek.count())
    ShipsPerDayOfWeek
      .coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .save("/tactical_prod1/raw/data_science_campus/ais/ShipsPerDayOfWeek_"+lat1+"_"+long1+"_"+lat2+"_"+long2+"__"+time1+"_"+time2+"_"+LatSegs+"_"+LongSegs )

    ShipsPerDayOfWeek = null
    //ShipsPerDayOfWeek.show()

      ais.unpersist()
      //println ("Number Of records in the rect:"+ais3.count())

    println("Done. All saved")
    spark.close()

  }

  def prepTileData ( A:(Double,Double),B:(Double,Double), t1:Int, t2:Int, in:DataFrame ):DataFrame = {
    val ais0 = in.filter(in.col("long") >= A._2.min(B._2) && in.col("long") <= A._2.max(B._2) && in.col("lat") >= A._1.min(B._1) && in.col("lat") <= A._1.max(B._1) && in.col("time") >= t1.min(t2) && in.col("time") <= t2.max(t1))
    val ais1 = ais0.withColumn("hours", (ais0("time")/3600).cast("Int"))
    val ais2 = ais1.withColumn("days", (ais1("hours")/24).cast("Int")).withColumn("TimeOfDay", (ais1("hours")/24).cast("Int"))
    val ais3 = ais2.withColumn("weeks", (ais2("days")/7).cast("Int")).withColumn("DayOfWeek", (ais2("days") % 7).cast("Int"))
    ais3
  }

  def calcShipsPerHour ( A:(Double,Double),B:(Double,Double), in:DataFrame ):DataFrame  = {
    val shipsPerHour = in.select(in("hours"),in("MMSI")).distinct().groupBy(in("hours")).count
    shipsPerHour
  }

  def calcShipsPerDay ( A:(Double,Double),B:(Double,Double),in:DataFrame ):DataFrame  = {
    val shipsPerDay = in.select(in("days"),in("MMSI")).distinct().groupBy(in("days")).count
    shipsPerDay
  }

  def calcShipsPerDayOfWeek ( A:(Double,Double),B:(Double,Double),in:DataFrame ):DataFrame  = {
    val shipsPerDayOfWeek = in.select(in("DayOfWeek"),in("MMSI")).distinct().groupBy(in("DayOfWeek")).count
    shipsPerDayOfWeek
  }

  def calcShipsPerTimeOfDay ( A:(Double,Double),B:(Double,Double),in:DataFrame ):DataFrame  = {
    val shipsPerDay = in.select(in("TimeOfDay"),in("MMSI")).distinct().groupBy(in("TimeOfDay")).count
    shipsPerDay
  }




  val dfStructSPH = new StructType(Array(
    StructField("hours",IntegerType, nullable = true),
    StructField("count",IntegerType, nullable = true),
    StructField("A_lat",DoubleType, nullable = true),
    StructField("A_lon",DoubleType, nullable = true),
    StructField("B_lat",DoubleType, nullable = true),
    StructField("B_lon",DoubleType, nullable = true)
  ))

  val dfStructSPD = new StructType(Array(
    StructField("days",IntegerType, nullable = true),
    StructField("count",IntegerType, nullable = true),
    StructField("A_lat",DoubleType, nullable = true),
    StructField("A_lon",DoubleType, nullable = true),
    StructField("B_lat",DoubleType, nullable = true),
    StructField("B_lon",DoubleType, nullable = true)
  ))

  val dfStructSPTD = new StructType(Array(
    StructField("TimeOfDay",IntegerType, nullable = true),
    StructField("count",IntegerType, nullable = true),
    StructField("A_lat",DoubleType, nullable = true),
    StructField("A_lon",DoubleType, nullable = true),
    StructField("B_lat",DoubleType, nullable = true),
    StructField("B_lon",DoubleType, nullable = true)
  ))

  val dfStructSPDW = new StructType(Array(
    StructField("DayOfWeek",IntegerType, nullable = true),
    StructField("count",IntegerType, nullable = true),
    StructField("A_lat",DoubleType, nullable = true),
    StructField("A_lon",DoubleType, nullable = true),
    StructField("B_lat",DoubleType, nullable = true),
    StructField("B_lon",DoubleType, nullable = true)
  ))


}
