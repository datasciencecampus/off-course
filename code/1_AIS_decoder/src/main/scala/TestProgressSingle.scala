package uk.gov.ons.dsc.trade

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._
import uk.gov.ons.dsc.trade.Filtering.{parseDouble, parseInt}

import scala.collection.mutable.ListBuffer

object TestProgressSingle {

  def main(args: Array[String]): Unit = {
    if (args.length != 8) {
      println("wrong number of parameters. They should be should be lat1,long1,lat2,long2,time1,time2, MMSI , est_period_secs... Exitting");
      return
    }

    val spark = SparkSession
      .builder()
      .master("yarn")
      //  .deploy-mode("client")
      .appName("AIS-ProgressIndicators")
      //.config("spark.dynamicAllocation.enabled","false")
      .config("spark.executor.memory", "15g")
      .config("spark.executor.cores", "6")
      .config("spark.executor.instances", "20")
      .getOrCreate()


    val lat1 = parseDouble(args(0)).getOrElse(51.0) // default coordinates
    val long1 = parseDouble(args(1)).getOrElse(1.0)

    val lat2 = parseDouble(args(2)).getOrElse(50.5)
    val long2 = parseDouble(args(3)).getOrElse(1.1)

    val timeA = parseInt(args(4)).getOrElse(0)
    val timeB = parseInt(args(5)).getOrElse(2147483647)

    val MMSI1 = parseInt(args(6)).getOrElse(0)

    val period = parseInt(args(7)).getOrElse(0)


    val time1 = timeA.min(timeB)
    val time2 = timeB.max(timeA)

    val latMin = lat1.min(lat2)
    val latMax = lat1.max(lat2)
    val longMin = long1.min(lat2)
    val longMax = long1.max(lat2)


    println("TestProgSingle with parameters:" + lat1 + "," + long1 + "," + lat2 + "," + long2 + " ,from:" + time1 + "s, to: " + time2 + " MMSI:" + MMSI1 + " period,s:"+ period)

    val ais = spark.sql(s"SELECT time, lat , long  , MMSI, SOG FROM parquet.`/tactical_prod1/raw/data_science_campus/ais/AIS_mess_type1`  WHERE time >= $time1 AND time <= $time2 AND lat>= $latMin AND lat <= $latMax AND long >= $longMin AND long <= $longMax AND MMSI = $MMSI1")
      .dropDuplicates(Seq("time", "MMSI", "long", "lat"))
      .cache()

    // calculate the minute slot for each message
    val ais1 = ais.withColumn("mins", (ais("time") / 60).cast(IntegerType))

    val aisMins = ais1.groupBy(ais1("mins")).min("time").as("time").sort("mins").select("min(time)")

    //aisMins.show()

    val timeList:Array[Int] = aisMins.collect().map(r=>r(0).asInstanceOf[Int])
    println("Size of the timeList: "+timeList.size)

    var listProgr:ListBuffer [(Int,Double)] =  new ListBuffer[(Int, Double)] ()

    val timeIT:Iterator[Int] = timeList.toIterator

    while(timeIT.hasNext) {
      val timeP1 = timeIT.next()
      val timeP2 = timeP1 - period


      println("computing period: "+timeP1+"-"+timeP2)
      val filtered = ais.filter(ais.col("MMSI") === MMSI1 && ais.col("time") <= timeP1 && ais.col("time") >= timeP2 )
        .sort("time").select("time","lat","long").collect()

      val progress:Double = ShipProgress1.compute(timeP1, timeP2, MMSI1, filtered)

      println("Progress ratio:"+progress)
      listProgr.append((timeP1,progress))
      println("Added to the list. List size:"+listProgr.size)
      println(" ")
    }



   println ("Final size of the list:"+ listProgr.size)


  /*

  // printing to a local file
   val file = new File ("Progr_" +MMSI1+"_"+period+".txt")
   file.createNewFile()
   val bw = new BufferedWriter( new FileWriter(file, true))
   listProgr.foreach(e=> bw.write(e._1.toString+","+e._2.toString))
   bw.close

  */



   val progRDD = spark.sparkContext.parallelize(listProgr.toList,1)

  // progRDD.saveAsTextFile("/tactical_prod1/raw/data_science_campus/ais/Progress" +MMSI1+"_"+period+".txt")


   val progDF = spark.createDataFrame(progRDD).toDF("time","prog")

   val aisProg = ais.filter(ais.col("MMSI") === MMSI1).join(progDF,Seq("time"))

    aisProg.select("time", "MMSI", "lat" , "long", "prog" , "SOG" )
           .write
           .mode(SaveMode.Overwrite)
           .save("/tactical_prod1/raw/data_science_campus/ais/ProgSingle")
    println (" DF saved ...")

    aisProg.select("time", "MMSI", "lat" , "long", "prog" , "SOG" )
      .write.format("com.databricks.spark.csv")
      .option("header","true")
      .mode(SaveMode.Overwrite)
      .save("/tactical_prod1/raw/data_science_campus/ais/ProgSingle" +MMSI1+"_"+period+".txt")

    println (" CSV saved ...")

    println (" Finished writing ! Closing ...")

    ais.unpersist(false)

    spark.close()

    /*
    ais
      .write.mode(SaveMode.Overwrite)
      .save("/tactical_prod1/raw/data_science_campus/ais/AIS_single_ship")

    println("All done. Number of points saved:"+ais.count())


    println("Number of records in the selected area:"+ais.count())

    val aisRdd = ais.map(r=> {
      val origColumns = r.toSeq.toList
      val time1 = origColumns(0).asInstanceOf[Int]
      val time2 = time1-60
      val MMSI = origColumns(3).asInstanceOf[Int]
      val params:String =   time1 + ":" + time2 + ":" + MMSI
      val counter = ais. count(_)
      //val prog1:Double = ShipProgress.compute(time1, time2, MMSI, ais)
      val prog1 = 1.0 // testing
      // assemble the new Row
      Row.fromSeq(origColumns ++ List(prog1, params, counter))
    })

    val ais1 = spark.createDataFrame(aisRdd, dfSchema1)

    ais1.show(50,false)

  }
  */

  }

}
