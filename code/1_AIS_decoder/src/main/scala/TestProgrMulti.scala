package uk.gov.ons.dsc.trade

// old file - do not use

import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType, StringType}
import org.apache.spark.sql.{Row, SparkSession}
import uk.gov.ons.dsc.trade.Filtering.{parseDouble, parseInt}

object TestProgrMulti {

  def main(args: Array[String]): Unit = {

    if (args.length != 6) {println("wrong number of parameters. They should be should be lat1,long1,lat2,long2,time1,time2 ... Exitting");return}

    val spark = SparkSession
      .builder()
      .master("yarn")
      //  .deploy-mode("client")
      .appName("AIS-TestProgrMulti")
      //.config("spark.dynamicAllocation.enabled","false")
      .config("spark.executor.memory","15g")
      .config("spark.executor.cores","6")
      .config("spark.executor.instances","20")
      .getOrCreate()


    val lat1 = parseDouble(args(0)).getOrElse(51.0) // default coordinates
    val long1 = parseDouble(args(1)).getOrElse(1.0)

    val lat2 = parseDouble(args(2)).getOrElse(50.5)
    val long2 = parseDouble(args(3)).getOrElse(1.1)

    val timeA = parseInt(args(4)).getOrElse(0)
    val timeB = parseInt(args(5)).getOrElse(2147483647)

    val time1 = timeA.min(timeB)
    val time2 = timeB.max(timeA)

    val latMin  = lat1.min(lat2)
    val latMax  = lat1.max(lat2)
    val longMin = long1.min(lat2)
    val longMax = long1.max(lat2)



    println("TestProgMulti with parameters:"+lat1+","+long1+","+lat2+","+long2+" ,from:"+time1+"s, to: "+time2)

    val ais = spark.sql(s"SELECT time, lat , long  , MMSI FROM parquet.`/tactical_prod1/raw/data_science_campus/ais/AIS_mess_type1`  WHERE time >= $time1 AND time <= $time2 AND lat>= $latMin AND lat <= $latMax AND long >= $longMin AND long <= $longMax ")
      .dropDuplicates(Seq("time","MMSI","long","lat"))
      .coalesce(120)
      .cache()

    println("Number of records in the selected area:"+ais.count())

    val aisRdd = ais.rdd.map(r=> {
      val origColumns = r.toSeq.toList
      val time2 = origColumns(0).asInstanceOf[Int] //to time
      val time1 = time2-60 // from time
      val MMSI = origColumns(3).asInstanceOf[Int]
      val params:String =   time1 + ":" + time2 + ":" + MMSI
      val counter = ais.count()
      val prog1:Double = ShipProgress.compute(time1, time2, MMSI, ais)
      //val prog1 = 1.0 // testing
      // assemble the new Row
      Row.fromSeq(origColumns ++ List(prog1, params, counter))
    })

    val ais1 = spark.createDataFrame(aisRdd, dfSchema1)

     ais1.show(50,false)

  }

  val dfSchema1 = new StructType(Array(

    StructField("time",IntegerType, nullable = true),
    StructField("lat",DoubleType, nullable = true),
    StructField("lon",DoubleType, nullable = true),
    StructField("MMSI",IntegerType, nullable = true),
    StructField("prog1",DoubleType, nullable = true),
    StructField("testParams",StringType, nullable = true)
  ))

}
