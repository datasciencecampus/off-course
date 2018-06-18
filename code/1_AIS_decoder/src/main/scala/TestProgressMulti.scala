package uk.gov.ons.dsc.trade

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import uk.gov.ons.dsc.trade.Filtering.{parseDouble, parseInt}

import scala.collection.mutable.ListBuffer

object TestProgressMulti {

  def main(args: Array[String]): Unit = {
    if (args.length != 8) {
      println("wrong number of parameters. They should be should be lat1,long1,lat2,long2,time1,time2, est_period_secs, starting_No... Exitting");
      return
    }

    val spark = SparkSession
      .builder()
      .master("yarn")
      //  .deploy-mode("client")
      .appName("AIS-ProgressIndicatorsMulti")
      //.config("spark.dynamicAllocation.enabled","false")
      .config("spark.executor.memory", "25g")
      .config("spark.executor.cores", "6")
      .config("spark.executor.instances", "20")
      .getOrCreate()


    val lat1 = parseDouble(args(0)).getOrElse(51.0) // default coordinates
    val long1 = parseDouble(args(1)).getOrElse(1.0)

    val lat2 = parseDouble(args(2)).getOrElse(50.5)
    val long2 = parseDouble(args(3)).getOrElse(1.1)

    val timeA = parseInt(args(4)).getOrElse(0)
    val timeB = parseInt(args(5)).getOrElse(2147483647)

    val period = parseInt(args(6)).getOrElse(0)
    val fromNo = parseInt(args(7)).getOrElse(0)


    val time1 = timeA.min(timeB)
    val time2 = timeB.max(timeA)
    val latMin = lat1.min(lat2)
    val latMax = lat1.max(lat2)
    val longMin = long1.min(long2)
    val longMax = long1.max(long2)


    val conf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    /*
    val InclMMSIList_exist = fs.exists(new org.apache.hadoop.fs.Path("/tactical_prod1/raw/data_science_campus/ais/MMSIsExcl.csv"))
    val MMSIsExcl = spark.read.textFile("/tactical_prod1/raw/data_science_campus/ais/MMSIsExcl.csv")

    */


    println("TestProgSingle with parameters:" + lat1 + "," + long1 + "," + lat2 + "," + long2 + " ,from:" + time1 + "s, to: " + time2 + " period,s:"+ period)

    val ais = spark.sql(s"SELECT time, lat , long  , MMSI, SOG FROM parquet.`/tactical_prod1/raw/data_science_campus/ais/AIS_mess_type1`  WHERE time >= $time1 AND time <= $time2 AND lat>= $latMin AND lat <= $latMax AND long >= $longMin AND long <= $longMax")
      .dropDuplicates(Seq("time", "MMSI", "long", "lat"))
      .cache()

    /*
    ais
      .write
      .mode(SaveMode.Overwrite)
      .save("/tactical_prod1/raw/data_science_campus/ais/aisFiltered")
    println (" intermediate ais saved ...")

    */

    // extract a list of MMSI in this area
    val MMSIs:Array[Int] = ais
                 .select("MMSI")
                 .distinct()
                 .collect()
                 .map(r=>r(0).asInstanceOf[Int])

    val numMMSIs = MMSIs.length
    var counter = 0
    println("Number of MMSIs found: "+ numMMSIs )

    var progAccDF:DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], dfStructAP) // main dataframe accumulator for the result

    var listProgr:ListBuffer [(Int,Double)] =  new ListBuffer[(Int, Double)] () // intermideate list accumulator for each time period

    val MMSI_It:Iterator[Int] = MMSIs.iterator

    // iterate through the MMSIs
    while (MMSI_It.hasNext) {
      val MMSI1 = MMSI_It.next()
      counter += 1
      if (counter >= fromNo) { // skip the first MMSIs - added to enable restarting of the job


      println ("### MMSI:"+ MMSI1 + "-->" + counter/numMMSIs*100 + "%")
      // calculate the minute slot for each message
      val ais1 = ais.filter(ais("MMSI") === MMSI1 ).withColumn("mins", (ais("time") / 60).cast(IntegerType))

      val aisMins = ais1.groupBy(ais1("mins")).min("time").as("time").sort("mins").select("min(time)")

      //aisMins.show()

      //collect the minutes in a list
      val timeList:Array[Int] = aisMins.collect().map(r=>r(0).asInstanceOf[Int])
      val sizeOfTimeList = timeList.size

      println("Size of the timeList: "+sizeOfTimeList)

      if (sizeOfTimeList < 4000 ) { //check to avoid tug boats, pilot vecels etc.

      listProgr =  new ListBuffer[(Int, Double)] () // resetting the list

      val timeIT:Iterator[Int] = timeList.toIterator

      while(timeIT.hasNext) {
        val timeP1 = timeIT.next()
        val timeP2 = timeP1 - period


        println(counter+":"+MMSI1+"->computing period: "+timeP1+"-"+timeP2 + " NumOfIntervals:"+sizeOfTimeList)
        val filtered = ais
                        .filter(ais.col("MMSI") === MMSI1 && ais.col("time") <= timeP1 && ais.col("time") >= timeP2 )
                        .select("time","lat","long")
                        //.sort("time")
                        .collect()
         //               .sortBy(r=>r.getInt(0))

        val progress:Double = ShipProgress1.compute(timeP1, timeP2, MMSI1, filtered)

        println("Progress ratio:"+progress)
        listProgr.append((timeP1,progress))
        println("Added to the list. List size:"+listProgr.size)
        println(" ")
      }

      println (" FInal Size of the progress list for MMSI:"+MMSI1+" is:"+ listProgr.size)

     val progRDD = spark.sparkContext.parallelize(listProgr.toList)

    // progRDD.saveAsTextFile("/tactical_prod1/raw/data_science_campus/ais/Progress" +MMSI1+"_"+period+".txt")
      println(" progRDD size:"+ progRDD.count())

     val progDF = spark.createDataFrame(progRDD).toDF("time","prog").withColumn("MMSI",lit(MMSI1))
      println(" progDF size:"+ progDF.count())

      println(" Old progAccDF size:"+ progAccDF.count())

     progAccDF = progAccDF.union(progDF)

      println(" New progAccDF size:"+ progAccDF.count())


     if (counter % 20 == 0) { // split MMSIs in batches of 20
       println(" ********* Saving 20 batch of ships *********")

       /*
       progAccDF
         .write
         .mode(SaveMode.Overwrite)
         .save("/tactical_prod1/raw/data_science_campus/ais/progAccDF")
       println (" intermediate progAccDF saved ...")

      */

       println("size of progAccDF:"+progAccDF.count())

       val aisProg = ais
                     .join(progAccDF,Seq("time","MMSI"))
                     .select("time", "MMSI", "lat" , "long", "prog" , "SOG" )
                     .coalesce(1)

       println("size of aisProg:"+aisProg.count())



       aisProg
         .write
         .mode(SaveMode.Append)
         .save("/tactical_prod1/raw/data_science_campus/ais/ProgMulti")
       println (" intermediate DF saved ...")

       aisProg
         .write.format("com.databricks.spark.csv")
         .option("header","false")
         .mode(SaveMode.Append)
         .save("/tactical_prod1/raw/data_science_campus/ais/ProgMulti_"+latMin+"_"+longMin+"_"+latMax+"_"+longMax+"_"+period+".csv")

       println (" CSV saved ...")

       // empty the accumulator dataframe
       progAccDF  = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], dfStructAP)
       }
      }
     }
    }


    // final saving
    val aisProg = ais
                   .join(progAccDF,Seq("time","MMSI"))
                   .select("time", "MMSI", "lat" , "long", "prog" , "SOG" )
                   .coalesce(1)

    aisProg
      .write
      .mode(SaveMode.Append)
      .save("/tactical_prod1/raw/data_science_campus/ais/ProgMulti")
    println (" final DF saved ...")

    aisProg
      .write.format("com.databricks.spark.csv")
      .option("header","false")
      .mode(SaveMode.Append)
      .save("/tactical_prod1/raw/data_science_campus/ais/ProgMulti_"+lat1+"_"+long1+"_"+lat2+"_"+long2+"_"+period+".txt")

    println (" final CSV saved ...")

   println (" final Finished writing ! Closing ...")

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

  val dfStructAP = new StructType(Array(
    StructField("time",IntegerType, nullable = true),
    StructField("prog",DoubleType, nullable = true),
    StructField("MMSI",IntegerType, nullable = true)
//    StructField("lat",IntegerType, nullable = true),
//    StructField("long",IntegerType, nullable = true),

//    StructField("SOG",DoubleType, nullable = true)
  ))

}
