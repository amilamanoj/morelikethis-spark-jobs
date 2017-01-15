name := "morelikethis-spark-jobs"

version := "1.0"

scalaVersion := "2.10.6"

lazy val spray = "1.3.3"
libraryDependencies ++= Seq (
  /*"spark.jobserver" % "job-server_2.10" % "0.7.0-SNAPSHOT",
  "com.typesafe.akka" %% "akka-actor" % "2.4.14" % "provided",
  "com.typesafe.akka" %% "akka-remote" % "2.4.14" % "provided",
  "com.typesafe.akka" %% "akka-agent" % "2.4.14" % "provided",
  "org.apache.spark" %% "spark-core" % "2.0.0" % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.0.0" % "provided",*/

  //"io.spray" % "spray-client_2.11" % "1.3.4",
    //"com.typesafe.play" %% "play-ws" % "2.4.3",
  //Requiredfor Scheduler
  "spark.jobserver" %% "job-server" % "0.7.0-SNAPSHOT",
  "com.typesafe.akka" %% "akka-slf4j" % "2.3.15",
  "org.apache.spark" %% "spark-core" % "2.0.0",
  "org.apache.spark" %% "spark-mllib" % "2.0.0",
  "com.typesafe.play" %% "play-json" % "2.4.3" ,
  "io.spray" %% "spray-client" % "1.3.3",
  "io.spray" %% "spray-http" % "1.3.3",
  "io.spray" %% "spray-json" % "1.3.3",
   "org.scalaj" %% "scalaj-http" % "2.3.0"

)
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }
