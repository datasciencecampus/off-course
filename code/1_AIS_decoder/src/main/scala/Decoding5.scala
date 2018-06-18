package uk.gov.ons.dsc.trade

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{SaveMode, SparkSession}



object Decoding5 {

  def main (args:Array[String]):Unit = {
    val spark = SparkSession
      .builder()
      .master("yarn")
      //  .deploy-mode("client")
      .appName("AIS-Decoding5")
      .config("spark.dynamicAllocation.enabled","false")
      .config("spark.executor.memory","12g")
      .config("spark.executor.cores","6")
      .config("spark.executor.instances","67")
      .getOrCreate()


    val aisRaw = spark.sql("SELECT * FROM parquet.`/tactical_prod1/raw/data_science_campus/bin/AISraw` WHERE mType = 5 ")

    val ais1 = aisRaw//.filter(aisRaw("mType") === 5)
      //.sample(false, .01)
      .coalesce(67)
      .withColumn("MMSI", getMMSI(aisRaw("message")) )
      .withColumn("IMO", getIMO(aisRaw("message")) )
      .withColumn("callSign", getCallSign(aisRaw("message")))
      .withColumn("shipName", getShipName(aisRaw("message")))
      .withColumn("shipType", getShipType(aisRaw("message")))
      .withColumn("to_bow", getToBow(aisRaw("message")))
      .withColumn("to_stern", getToStern(aisRaw("message")))
      .withColumn("to_port", getToPort(aisRaw("message")))
      .withColumn("to_starboard", getToStarboard(aisRaw("message")))
      .withColumn("epfd", getPosFixType(aisRaw("message")))
      .withColumn("ETA_month", getETAmonth(aisRaw("message")))
      .withColumn("ETA_day", getETAday(aisRaw("message")))
      .withColumn("ETA_hour", getETAhour(aisRaw("message")))
      .withColumn("ETA_min", getETAmin(aisRaw("message")))
      .withColumn("Draught", getDraught(aisRaw("message")))
      .withColumn("Dest", getDestination(aisRaw("message")))



    ais1.repartition(1).write.mode(SaveMode.Overwrite).save("/tactical_prod1/raw/data_science_campus/ais/AIS_mess_type5")

    println("Size of data saved in AIS5temp:" + ais1.count)

    spark.close()



  }

  val getIMO = udf [Int , String] ( x=> parseIntSafe (x.slice(40,70)).getOrElse(0) )
  val getMMSI = udf [Int , String] ( x=> parseIntSafe( x.slice(8,38)).getOrElse(0) )
  val getCallSign = udf [String , String] ( x=>  x.slice(70,112))
  val getShipName = udf [String , String] ( x=> x.slice(112,232))
  val getShipType = udf [Int , String] ( x=> parseIntSafe( x.slice(232,240)).getOrElse(0) )
  val getToBow = udf [Int , String] ( x=> parseIntSafe( x.slice(240,249)).getOrElse(0) )
  val getToStern = udf [Int , String] ( x=> parseIntSafe( x.slice(249,258)).getOrElse(0) )
  val getToPort = udf [Int , String] ( x=> parseIntSafe( x.slice(258,264)).getOrElse(0) )
  val getToStarboard = udf [Int , String] ( x=> parseIntSafe( x.slice(264,270)).getOrElse(0) )
  val getPosFixType = udf [Int , String] ( x=> parseIntSafe( x.slice(270,274)).getOrElse(0) )
  val getETAmonth = udf [Int , String] ( x=> parseIntSafe( x.slice(274,278)).getOrElse(0) )
  val getETAday = udf [Int , String] ( x=> parseIntSafe( x.slice(278,283)).getOrElse(0) )
  val getETAhour = udf [Int , String] ( x=> parseIntSafe( x.slice(283,288)).getOrElse(0) )
  val getETAmin = udf [Int , String] ( x=> parseIntSafe( x.slice(288,294)).getOrElse(0) )
  val getDraught = udf [Int , String] ( x=> parseIntSafe( x.slice(294,302)).getOrElse(0) )
  val getDestination = udf [String , String] ( x=> x.slice(302,422))


  def parseIntSafe (s:String):Option [Int] = {
    try {
      Some (Integer.parseInt(s,2))
    } catch {
      case e:Exception => None
    }
  }

}
