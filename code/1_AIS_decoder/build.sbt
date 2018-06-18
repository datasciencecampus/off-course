name := "shipping-trade"

scalaVersion := "2.11.10"


lazy val root = (project in file(".")).
  settings (
    name := "AIS",
    version := "0.1",
    scalaVersion := "2.11.10",
    mainClass in Compile := Some ("uk.gov.ons.dsc.trade.Ingestion")


  )


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0"  ,
  "org.apache.spark" %% "spark-sql" % "2.1.0"  ,
  "org.apache.spark" %% "spark-mllib" % "2.1.0"
)
        