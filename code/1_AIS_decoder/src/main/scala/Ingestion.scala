package uk.gov.ons.dsc.trade

// reads the input CSV files

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{StructField, StructType, _}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import uk.gov.ons.dsc.trade.Filtering.parseInt

object Ingestion {

  def main (args:Array[String]):Unit = {


    val spark = SparkSession
      .builder()
      .master("yarn")
      //  .deploy-mode("client")
      .appName("AIS-Ingestion")
      .config("spark.dynamicAllocation.enabled","false")
      .config("spark.executor.memory","20g")
      .config("spark.executor.cores","5")
      .config("spark.executor.instances","10")
      .getOrCreate()


    //read the filenames in the directory
    val files = FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus ( new Path("/tactical_prod1/raw/data_science_campus/ais")).map(line=>line.getPath.toString)

    println("Found "+files.size+ " files in the folder")
    var ais =     spark.createDataFrame(spark.sparkContext.emptyRDD[Row], dfStruct1)
    var counter:Int = 1

    files.foreach(filename => procFile(filename))

    ais.coalesce(30).write.mode(SaveMode.Append).save("/tactical_prod1/raw/data_science_campus/bin/AISraw")


    def procFile (file:String) {

      val date = parseInt(file.slice(72,74)).getOrElse(0)

      if (date >=21 && file.slice(72,78) !="22_May") {
      println("processing file No:"+counter+" -> "+file.takeRight(31))

      val aisRaw = spark.sparkContext.textFile(file)

      val aisRaw1 = aisRaw.map(line => line.split(","))
      // println("Size of aisRaw1:" + aisRaw1.count)
      // println("#2")
      val aisRaw2 = aisRaw1.filter(line => line(2).equals("2")) // only the messagies that are made of two lines
      // println("#3")
      val aisRaw2I = aisRaw2.zipWithIndex()
      val aisRaw3 = aisRaw2I.map(l => (l._1(0).slice(13, 29) + l._1(1).slice(0, 13) + (l._2 / 2).toString, l._1))
      // println("#3a")
      //println("#4")
      // println("Size of aisRaw3:" + aisRaw3.count)
      val aisRaw4 = aisRaw3.reduceByKey((x, y) => Array(x(0), x(1), x(1), x(2), x(3), x(4), x(5), x(6) + y(6), ""))
      //println("#5")
      //println("Size of aisRaw4:" + aisRaw4.count)
      //println("#6")
      val aisRaw5 = aisRaw4.map(line => line._2)
      //println("#7")

      val aisRaw6 = aisRaw1.filter(line => line(2) != "2")
        .union(aisRaw5)
        .map(l => Seq(toIntSafe(timePat.findFirstIn(l(1)).getOrElse("0")).getOrElse(0), toIntSafe(portPat.findFirstIn(l(0)).getOrElse("0")).getOrElse(0), toIntSafe(shipPat.findFirstIn(l(0)).getOrElse("0")).getOrElse(0), l(6).map(ch => ch.toInt).map(e => e - 48).map { case e if e > 40 => e - 8; case e => e }.map(e => String.format("%6s", Integer.toBinaryString(e)).replace(' ', '0')).mkString,l(6),l(7)))
        .map(r => Row.fromSeq(r))
      //println("#8")
      println("The first row of data:" + aisRaw6.first())

      val aisDF0 = spark.sqlContext.createDataFrame(aisRaw6, dfStruct)
      val aisDF1 = aisDF0.withColumn("mType", getMessageType(aisDF0("message")) )

      //aisDF1.write.mode(SaveMode.Append).save("/tactical_prod1/raw/data_science_campus/bin/AISraw")

      ais = ais.union(aisDF1)

      println("Size of data found: " + aisDF1.count)


      if (counter >= 30) {
        ais.coalesce(30).write.mode(SaveMode.Append).save("/tactical_prod1/raw/data_science_campus/bin/AISraw")
        println ("Batch written to disk")
        ais =     spark.createDataFrame(spark.sparkContext.emptyRDD[Row], dfStruct1)
        counter = 1
      }
      else
        counter = counter + 1

      }

      else
        println("skipping file No:"+counter+" -> "+file.takeRight(31))

      }



    spark.close()


  }


  val portPat = "(?<=Port=)([0-9]+)".r
  val shipPat = "(?<=MMSI=)([0-9]+)".r
  val timePat = "(?<=c:)([0-9]+)".r


  val dfStruct = new StructType(Array(
    StructField("time",IntegerType, nullable = true),
    StructField("port",IntegerType, nullable = true),
    StructField("ship",IntegerType, nullable = true),
    StructField("message",StringType, nullable = true),
    StructField("origMess",StringType, nullable = true),
    StructField("checkSum",StringType, nullable = true)
  ))

  val dfStruct1 = new StructType(Array(
    StructField("time",IntegerType, nullable = true),
    StructField("port",IntegerType, nullable = true),
    StructField("ship",IntegerType, nullable = true),
    StructField("message",StringType, nullable = true),
    StructField("origMess",StringType, nullable = true),
    StructField("checkSum",StringType, nullable = true),
    StructField("mType",IntegerType, nullable = true)

  ))


  val getMessageType = udf [Int , String] ( x=> Integer.parseInt( x.substring(0,6) ,2) )

  def toIntSafe (s:String):Option [Int] = {
    try {
      Some (s.toInt)
    } catch {
      case e:Exception => None
    }
  }
}


