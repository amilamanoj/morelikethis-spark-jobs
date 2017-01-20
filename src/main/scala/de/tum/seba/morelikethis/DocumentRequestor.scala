package de.tum.seba.morelikethis

import java.util.concurrent.Executors

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import spark.jobserver.{SparkJob, SparkJobInvalid, SparkJobValid, SparkJobValidation}

import scala.util.Try
import spray.http._
import spray.client.pipelining._
import spray.httpx.encoding.Deflate
import spray.json.JsonParser
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * Created by Prateek Bagrecha on 1/15/2017.
  */
object DocumentRequestor{

  val baseUrl: String = "https://server.sociocortex.com/api/v1"
  val authUrl: String = baseUrl + "/jwt"
  val wsUrl: String = baseUrl + "/workspaces"
  val wsNwUrl: String = wsUrl + "/northwind"
  val entityTypNwUrl: String = wsNwUrl + "/entityTypes"
  val entitiesNwUrl: String = wsNwUrl + "/entities"
  val entityExNwUrl: String = baseUrl + "/entities/teatime"
  val prodNwUrl: String = baseUrl + "/entityTypes/nwproduct"

  def getValueFromHttpEntity(jsonKey: String, entityResult: HttpEntity): String ={
    val httpJsonMap = JsonParser(entityResult.asString).asJsObject()

    val keyMap = httpJsonMap.getFields(jsonKey)
    keyMap.map(_.toString()).mkString("\n").toString
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[4]").setAppName("VectorBuilderJob")
    val sc = new SparkContext(conf)
    val config = ConfigFactory.parseString("")
    val pool = Executors.newCachedThreadPool()
    implicit val ec = ExecutionContext.fromExecutorService(pool)
    val future = Future {
      runJob(sc, config)
    }
    val promise = Await.ready(future, 50 seconds)
    promise.onSuccess {
      case result =>  println("Result: "+ result)

    }
    //val result = runJob(sc, config)
    //println("Result: "+ result)
  }

  def validate(sc: SparkContext, config: Config) = {
    Try(config.getString("input.string"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No input.string config param"))
  }

  def runJob(sc: SparkContext, config: Config): Any = {

    var entityId = ""
    var entityContent = ""


    implicit val JwtSystem = ActorSystem("JwtSystem")
    import JwtSystem.dispatcher
    // execution context for futures

    val jwtPipeline: HttpRequest => Future[HttpResponse] = (
      addCredentials(BasicHttpCredentials("prateek.bagrecha@outlook.com", "Bakuman057"))
        ~> sendReceive
        ~> decode(Deflate)
        ~> unmarshal[HttpResponse]
      )

    val jwtResponse: Future[HttpResponse] = jwtPipeline(Post(authUrl))

    jwtResponse.onFailure {
      case result =>
        println("Jwt Request Failure")
        println(result.getMessage)
    }

    jwtResponse.onSuccess {
      case jwtResult =>
        println("Jwt Request Success")
        val response: Future[HttpResponse] = jwtPipeline(Get("https://server.sociocortex.com/api/v1/entities/ose32apj5pa9"))

        response.onFailure {
          case entityResult =>
            println("Get Entity : Http Request Failed")
            println(entityResult.getMessage)
            JwtSystem.shutdown()
        }

        response.onSuccess {
          case entityResult =>
            println("Http Request Success")
            println(entityResult.entity)
            entityContent = getValueFromHttpEntity("content", entityResult.entity)
            println("Entity Content: "+ entityContent)
            JwtSystem.shutdown()
        }
    }
    entityContent
  }

}
