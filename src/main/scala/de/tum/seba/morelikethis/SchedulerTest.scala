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

class Init {

  def InitSparkContext: SparkContext = {
    val conf = new SparkConf().setAppName("VecJobScheduler")
      .setMaster("local[4]")
    SparkContext.getOrCreate(conf)
  }

}

class testActor extends ActorStack {

  override def wrappedReceive = {
    case "RunVectorBuildJob" =>
      println("Actor: Received Message to Run Vector Build Job")
      println("Actor: Creating/Geting Existing Spark Context")
      val sc = new Init().InitSparkContext
      println("................Triggering Vector Builder Job................")
      VectorBuilderJob.runJob(sc, ConfigFactory.parseString("input.string = Run Vector Build Job "))
    case y: Long =>
      println("Actor: Time Received: " + y + "\n")
    case _ =>
      println("Actor: Received Unknown Message :" + _)
  }
}


object SchedulerTest extends SparkJob {

  def main(args: Array[String]): Unit = {
    val sc = new Init().InitSparkContext
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

    val sc_config = ConfigFactory.load("my.conf")
    val system = akka.actor.ActorSystem("system")
    val ta = system.actorOf(Props[testActor], name = "ta")

    val delay = Duration(sc_config.getString("sc.run-job.delay")).asInstanceOf[FiniteDuration]
    val frequency = Duration(sc_config.getString("sc.run-job.frequency")).asInstanceOf[FiniteDuration]

    system.scheduler.schedule(delay, frequency) {
      println("Scheduler RunJob: Vector Calculation Job will been Scheduled")
      println("Scheduler RunJob: Vector Calculation Job will run with ..")
      println("Scheduler RunJob: Initial Delay: " + delay)
      println("Scheduler RunJob: Frequency of Run: " + frequency)
      println("Scheduler RunJob: Send Message to Run Vector Job")
      ta ! config.getString("input.string")
    }

  }

}
