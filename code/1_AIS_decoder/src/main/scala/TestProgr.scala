package uk.gov.ons.dsc.trade

import org.apache.spark.sql.SparkSession

object TestProgr {

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      println("wrong parameters! Usage: from_time0 to_time1 MMSI ");
      return
    }

    val time0 = Utils.parseInt(args(0)).getOrElse(0)
    val time1 = Utils.parseInt(args(1)).getOrElse(2147483647)
    val ship  = Utils.parseInt(args(2)).getOrElse(0)

    val spark = SparkSession
      .builder()
      .master("yarn")
      //  .deploy-mode("client")
      .appName("AIS-Extraction")
     // .config("spark.dynamicAllocation.enabled", "false")
      .config("spark.executor.memory", "15g")
      .config("spark.executor.cores", "6")
      .config("spark.executor.instances", "10")
      .getOrCreate()

    val ais = spark.sql(s"SELECT time , long , lat , MMSI FROM parquet.`/tactical_prod1/raw/data_science_campus/ais/AIS_mess_type1` WHERE MMSI = $ship  AND time >= $time0 AND time <= $time1")
      .coalesce(60).cache()

    println ("Calculating the shipProgress with parameters:"+time0+":"+time1+":"+ship)

    val progr = ShipProgress.compute(time0, time1, ship, ais)

    println("Result for MMSI:"+ ship + " is:" +progr)

    ais.unpersist(false)


  }


}
