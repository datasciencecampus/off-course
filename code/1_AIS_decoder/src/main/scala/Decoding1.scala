package uk.gov.ons.dsc.trade

// Decodes messagies of type 1 from the raw data

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._


object Decoding1 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("yarn")
      //  .deploy-mode("client")
      .appName("AIS-Decoding1")
      .config("spark.dynamicAllocation.enabled","false")
      .config("spark.executor.memory","12g")
      .config("spark.executor.cores","6")
      .config("spark.executor.instances","67")
      .getOrCreate()


    //val aisRaw = spark.read.load("AISraw")

    val aisRaw = spark.sql("SELECT * FROM parquet.`/tactical_prod1/raw/data_science_campus/bin/AISraw` WHERE mType = 1 ")

    val ais0 = aisRaw
      .coalesce(402)

    val ais1 = ais0
      //.cache ()
      //.sample(false, .001)
      .withColumn("long", getLong(ais0("message")))
      .withColumn("lat", getLat(ais0("message")))
      .withColumn("MMSI", getMMSI(ais0("message")))
      .withColumn("NavStat", getNavStat(ais0("message")))
      .withColumn("ROT", getROT(ais0("message")))
      .withColumn("SOG", getSOG(ais0("message")))
      .withColumn("COG", getCOG(ais0("message")))
      .withColumn("HDG", getHDG(ais0("message")))
      .withColumn("TimeStamp", getTimeStamp(ais0("message")))


    ais1
        .sort("lat","long")
        .write.mode(SaveMode.Overwrite)
        .save("/tactical_prod1/raw/data_science_campus/ais/AIS_mess_type1")

    println("Size of data saved in AIS_type1: " + ais1.count)

    spark.close()


  }

  val getLong = udf[Double, String](x => (twoCompl(x.slice(61, 89)).getOrElse(0.0)) / 600000)
  val getLat = udf[Double, String](x => (twoCompl(x.slice(89, 116)).getOrElse(0.0)) / 600000)
  val getMMSI = udf[Int, String](x => parseIntSafe(x.slice(8, 38)).getOrElse(0))
  val getNavStat = udf[Int, String](x => parseIntSafe(x.slice(38, 42)).getOrElse(0))
  val getROT = udf[Double, String](x => 5.619047*twoCompl(x.slice(42, 50)).getOrElse(0.0))
  val getSOG = udf[Double, String](x => parseIntSafe(x.slice(50, 60)).getOrElse(0)/10.0)
  val getCOG = udf[Double, String](x => parseIntSafe(x.slice(116, 128)).getOrElse(0)/10.0)
  val getHDG = udf[Int, String](x => parseIntSafe(x.slice(128, 137)).getOrElse(0))
  val getTimeStamp = udf[Int, String](x => parseIntSafe(x.slice(137, 143)).getOrElse(0))

  def twoCompl(s: String): Option[Double] = {
    import scala.math.pow

    val i1= parseIntSafe(s).getOrElse(0)
    if (i1> pow (2,s.length-1) ) {
      Some(i1 - pow (2,s.length))
    }
    else {
      Some(i1)
    }

  }



  def parseIntSafe (s:String):Option [Int] = {
    try {

        Some (Integer.parseInt(s,2))

    } catch {
      case e:Exception => None
    }
  }

  /*
  def sq (x:Double):Int = {
    if (x>= 0 )
      (x*x).toInt
    else
      -(x*x).toInt
  }
  */


}

