package uk.gov.ons.dsc.trade

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.udf

object Decoding4 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("yarn")
      //  .deploy-mode("client")
      .appName("AIS-Decoding4")
      .config("spark.dynamicAllocation.enabled","false")
      .config("spark.executor.memory","12g")
      .config("spark.executor.cores","6")
      .config("spark.executor.instances","67")
      //  .set("spark.cores.max", "25")
      //.config("spark.some.config.option", "some-value")
      .getOrCreate()


    val aisRaw = spark.sql("SELECT * FROM parquet.`/tactical_prod1/raw/data_science_campus/bin/AISraw` WHERE mType = 4 ")

    val ais4 = aisRaw
      //.filter(aisRaw("mType") === 4)
      //.sample(false, .001)
      .coalesce(402)
      .withColumn("long", getLong(aisRaw("message")))
      .withColumn("lat", getLat(aisRaw("message")))
      .withColumn("MMSI", getMMSI(aisRaw("message")))
      .withColumn("year", getYear(aisRaw("message")))
      .withColumn("month", getMonth(aisRaw("message")))
      .withColumn("day", getDay(aisRaw("message")))
      .withColumn("hour", getHour(aisRaw("message")))
      .withColumn("minute", getMinute(aisRaw("message")))
      .withColumn("repeatInd", getRepeatInd(aisRaw("message")))
      .withColumn("fixType", getFixType(aisRaw("message")))


    ais4.sort("lat","long").write.mode(SaveMode.Overwrite).save("/tactical_prod1/raw/data_science_campus/ais/AIS_mess_type4")

    println("Size of data saved in AIS_type4: " + ais4.count)

    spark.close()


  }

  val getLong = udf[Double, String](x => (twoCompl(x.slice(79, 107)).getOrElse(0.0)) / 600000)
  val getLat = udf[Double, String](x => (twoCompl(x.slice(107, 134)).getOrElse(0.0)) / 600000)
  val getMMSI = udf[Int, String](x => parseIntSafe(x.slice(8, 38)).getOrElse(0))
  val getYear = udf[Int, String](x => parseIntSafe(x.slice(38, 52)).getOrElse(0))
  val getMonth = udf[Int, String](x => parseIntSafe(x.slice(52, 56)).getOrElse(0))
  val getDay = udf[Int, String](x => parseIntSafe(x.slice(56, 61)).getOrElse(0))
  val getHour = udf[Int, String](x => parseIntSafe(x.slice(61, 66)).getOrElse(0))
  val getMinute = udf[Int, String](x => parseIntSafe(x.slice(67, 72)).getOrElse(0))
  val getRepeatInd = udf[Int, String](x => parseIntSafe(x.slice(6, 8)).getOrElse(0))
  val getFixType = udf[Int, String](x => parseIntSafe(x.slice(134, 138)).getOrElse(0))

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

  def sq (x:Double):Int = (x*x).toInt

}

