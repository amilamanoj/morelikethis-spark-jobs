name := "morelikethis-spark-jobs"

version := "1.0"

scalaVersion := "2.11.8"


libraryDependencies ++= Seq (
  "org.apache.spark" %% "spark-core" % "2.0.0"  % "provided" ,
  "org.apache.spark" %% "spark-mllib" % "2.0.0"  % "provided" ,
  "spark.jobserver" % "job-server_2.10" % "0.7.0-SNAPSHOT" % "provided",
  //  "io.spray" % "spray-client_2.11" % "1.3.4"
  //    "com.typesafe.play" %% "play-ws" % "2.4.3",
  //  "com.typesafe.play" %% "play-json" % "2.4.3" ,
  "org.scalaj" % "scalaj-http_2.10" % "2.3.0" % "provided"
)
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }
