package uk.gov.ons.dsc.trade

// Extracts a number of statistical papams in a grid for rec. made of two points,i.e. top left (TL) and bottom right (BR)

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Filtering {


  def main(args: Array[String]): Unit = {

    if (args.length != 6) {println("wrong number of parameters. They should be should be lat1,long1,lat2,long2,time1,time2 ... Exitting");return}

    val spark = SparkSession
      .builder()
      .master("yarn")
      //  .deploy-mode("client")
      .appName("AIS-Filterring")
      //.config("spark.dynamicAllocation.enabled","false")
      .config("spark.executor.memory","15g")
      .config("spark.executor.cores","6")
      .config("spark.executor.instances","20")
      .getOrCreate()


    val lat1 = parseDouble(args(0)).getOrElse(51.0) // default coordinates
    val long1 = parseDouble(args(1)).getOrElse(1.0)

    val lat2 = parseDouble(args(2)).getOrElse(50.5)
    val long2 = parseDouble(args(3)).getOrElse(1.1)

    val latMin = lat1.min(lat2)
    val latMax = lat1.max(lat2)

    val longMin = long1.min(long2)
    val longMax = long1.max(long2)


    val timeA = parseInt(args(4)).getOrElse(0)
    val timeB = parseInt(args(5)).getOrElse(2147483647)

    val time1 = timeA.min(timeB)
    val time2 = timeB.max(timeA)

    println("Filtering with parameters:"+lat1+","+long1+","+lat2+","+long2+" ,from:"+time1+"s, to: "+time2)

    val ais = spark.sql(s"SELECT time , ship, long , lat , MMSI, NavStat, ROT, SOG, COG, HDG, TimeStamp FROM parquet.`/tactical_prod1/raw/data_science_campus/ais/AIS_mess_type1`  WHERE time >= $time1 AND time <= $time2 AND lat >=$latMin AND lat <= $latMax AND long >= $longMin AND long <= $longMax")
      .coalesce(120).cache()

    val ais0:DataFrame = ais//.filter(ais.col("long") >= long1.min(long2) && ais.col("long") <= long2.max(long1) && ais.col("lat") >= lat1.min(lat2) && ais.col("lat") <= lat2.max(lat1) && ais.col("time") >= time1 && ais.col("time") <= time2)
                    .dropDuplicates(Seq("time","MMSI","long","lat"))

    //val wSpec1 = Window.partitionBy("MMSI").orderBy("time").rowsBetween(-100,0)

    //val ais1 = ais0.withColumn("", )





    println("Number of records in the tile:"+ais0.count())
    ais0.coalesce(1)
           .write.format("com.databricks.spark.csv")
           .option("header","true")
           .mode(SaveMode.Overwrite)
           .save("/tactical_prod1/raw/data_science_campus/ais/filteredAIS_"+lat1+"_"+long1+"_"+lat2+"_"+long2+"_"+time1+"_"+time2)

    println ("results saved in:/tactical_prod1/raw/data_science_campus/ais/filteredAIS_"+lat1+"_"+long1+"_"+lat2+"_"+long2+"_"+time1+"_"+time2)

    spark.close()

  }




  def parseDouble (s:String) = try {Some(s.toDouble)} catch { case _: Throwable => None}
  def parseInt (s:String) = try {Some(s.toInt)} catch { case _: Throwable => None}

}
