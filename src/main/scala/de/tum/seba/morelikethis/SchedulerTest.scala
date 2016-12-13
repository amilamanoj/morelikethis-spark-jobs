package de.tum.seba.morelikethis

/**
  * Created by Prateek Bagrecha on 13.12.2016.
  */
import akka.actor.Props
import com.typesafe.config.{Config, ConfigFactory}
import ooyala.common.akka.ActorStack
import org.apache.spark.{SparkConf, SparkContext}
import spark.jobserver.{SparkJob, SparkJobInvalid, SparkJobValid, SparkJobValidation}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Try

class testActor extends ActorStack {

  override def wrappedReceive = {
    case x: String => printf("Received String: " + x + "\n")
    case y: Long => printf("Time Received: " + y + "\n")
    case _ => printf("received unknown message" + "\n")
  }
}


object SchedulerTest extends SparkJob {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("SchedulerTest")
    val sc = new SparkContext(conf)
    val config = ConfigFactory.parseString(args(0))
    println(config)
    val results = runJob(sc, config)
    println("Result is " + results)
  }

  def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("input.string"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No input.string config param"))
  }

  def runJob(sc: SparkContext, config: Config): Any = {

    val system = akka.actor.ActorSystem("system")
    val ta = system.actorOf(Props[testActor], name = "ta")

    system.scheduler.schedule(50 milliseconds, 15 seconds) {
      ta ! config.getString("input.string")
    }

    system.scheduler.schedule(50 milliseconds, 15 seconds) {
      ta ! System.currentTimeMillis
    }

  }

}
