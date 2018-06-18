package uk.gov.ons.dsc.trade

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable.MutableList

object GridStatsSPH {

  def main(args: Array[String]): Unit = {

    if (args.length != 8) {println("wrong parameters! Usage: latA lonA latB longB timeFrom timeTo segmentsLat segmentsLong");return}

    val spark = SparkSession
      .builder()
      .master("yarn")
      //  .deploy-mode("client")
      .appName("AIS-GridStatsSPH")
      .config("spark.dynamicAllocation.enabled","false")
      .config("spark.executor.memory","15g")
      .config("spark.executor.cores","6")
      .config("spark.executor.instances","30")
      .getOrCreate()


    // parcing the parameters
    val lat1 = parseDouble(args(0)).getOrElse(51.0)
    val long1 = parseDouble(args(1)).getOrElse(1.0)

    val lat2 = parseDouble(args(2)).getOrElse(50.0)
    val long2 = parseDouble(args(3)).getOrElse(2.0)

    val time1 = parseInt(args(4)).getOrElse(0)
    val time2 = parseInt(args(5)).getOrElse(2147483647)

    val LatSegs:Int = parseInt(args(6)).getOrElse(1)
    val LongSegs:Int = parseInt(args(7)).getOrElse(1)


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

    var DFcounter:Int = 0

    val grid:Array[Array[(Double,Double)]] = Array.tabulate [(Double,Double)](LatSegs+1,LongSegs+1) ((x,y)=> (lat1+x*LatInc,long1+y*LongInc))


    val input = spark.sql("SELECT time , port, ship, long , lat , MMSI, NavStat, ROT, SOG, COG, HDG, TimeStamp FROM parquet.`/tactical_prod1/raw/data_science_campus/ais/AIS_mess_type1`  ")


    val ais = input.filter(input("long") >= long1.min(long2) && input("long") <= long2.max(long1) && input("lat") >= lat1.min(lat2) && input("lat") <= lat2.max(lat1) && input("time") >= time1.min(time2) && input("time") <= time2.max(time1))
      .coalesce(180)
      .cache()


   // var ShipsPerHour  = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], dfStructSPH)

    var tileStats:MutableList[DataFrame] = new MutableList()

    /////////////////////// ShipsPerHour
    for (i <- 0 to LatSegs-1; j <- 0 to LongSegs-1) {    // iterate through the tiles
      println("Processing cell:("+i+","+j+") with coordinates TL:"+ grid(i)(j) + " BR:" + grid(i+1)(j+1) )
      val ais10 = prepTileData (grid(i)(j),grid(i+1)(j+1),time1,time2,ais)  //.cache()
      val numRecs = ais10.count

      println("Number of records found in the cell: " + numRecs)

      if (numRecs > 0 ) {

        println("calcShipsPerHour ...")
        // ShipsPerHour = ShipsPerHour.union(calcShipsPerHour (grid(i)(j),grid(i+1)(j+1),ais10).withColumn("A_lat",lit (grid(i)(j)._1)).withColumn("A_lon",lit (grid(i)(j)._2)).withColumn("B_lat",lit (grid(i+1)(j+1)._1)).withColumn("B_lon",lit (grid(i+1)(j+1)._2)) )
        tileStats += calcShipsPerHour(grid(i)(j), grid(i + 1)(j + 1), ais10).withColumn("A_lat", lit(grid(i)(j)._1)).withColumn("A_lon", lit(grid(i)(j)._2)).withColumn("B_lat", lit(grid(i + 1)(j + 1)._1)).withColumn("B_lon", lit(grid(i + 1)(j + 1)._2))

        if (DFcounter > 100) {
          println ("Temp. saving of data Reducing the number of Dataframes from:"+ tileStats.size)
          val ShipsPerHour = tileStats.reduce((a,b)=> a.union(b))

          ShipsPerHour
            .coalesce(1)
            .write.mode(SaveMode.Append)
            .save("/tactical_prod1/raw/data_science_campus/ais/ShipsPerHour_"+lat1+"_"+long1+"_"+lat2+"_"+long2+"__"+time1+"_"+time2+"_"+LatSegs+"_"+LongSegs )


          tileStats.clear()
          DFcounter = 0
        }

      } else {
        println("Skippinhg ...")
      }
    }
    println ("Reducing the number of Dataframes from:"+ tileStats.size)
    val ShipsPerHour = tileStats.reduce((a,b)=> a.union(b))

    println ("Final calcShipsPerHour ..."+ShipsPerHour.count)


    ShipsPerHour
      .coalesce(1)
      .write.mode(SaveMode.Append)
      .save("/tactical_prod1/raw/data_science_campus/ais/ShipsPerHour_"+lat1+"_"+long1+"_"+lat2+"_"+long2+"__"+time1+"_"+time2+"_"+LatSegs+"_"+LongSegs )



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


  def parseDouble (s:String) = try {Some(s.toDouble)} catch { case _: Throwable => None}
  def parseInt (s:String) = try {Some(s.toInt)} catch { case _: Throwable => None}


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
