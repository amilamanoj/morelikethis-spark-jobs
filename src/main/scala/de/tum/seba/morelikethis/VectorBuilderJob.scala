package de.tum.seba.morelikethis

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import spark.jobserver.{SparkJob, SparkJobInvalid, SparkJobValid, SparkJobValidation}

import scala.util.Try
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import play.libs.Json

import scala.concurrent.ExecutionContext.Implicits.global
import scalaj.http.HttpResponse
import scalaj.http._

object VectorBuilderJob extends SparkJob {

  val authUrl: String = "http://localhost:8083/intern/tricia/api/v1/jwt"
  val wsUrl: String = "http://localhost:8083/intern/tricia/api/v1/workspaces"
  val wsNwUrl: String = "http://localhost:8083/intern/tricia/api/v1/workspaces/northwind"
  val entityTypNwUrl: String = "http://localhost:8083/intern/tricia/api/v1/workspaces/northwind/entityTypes"
  val entitiesNwUrl: String = "http://localhost:8083/intern/tricia/api/v1/workspaces/northwind/entities"
  val entityExNwUrl: String = "http://localhost:8083/intern/tricia/api/v1/entities/teatime"
  val prodNwUrl: String = "http://localhost:8083/intern/tricia/api/v1/entityTypes/nwproduct"

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[4]").setAppName("VectorBuilderJob")
    val sc = new SparkContext(conf)
    val config = ConfigFactory.parseString("")

    val results = runJob(sc, config)
    println("Result is " + results)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("input.string"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No input.string config param"))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {

    val authResponse: HttpResponse[String] = Http(authUrl).postData("{\n \"username\":\"mustermann@test.sc\",\n \"password\":\"ottto\"\n}").header("content-type", "application/json").asString
    val authJson = Json.parse(authResponse.body)
    val token = authJson.at("/token").asText()
    println(token)

    // todo: loop all documents and get text content

    val res: HttpResponse[String] = Http(entityExNwUrl).header("Authorization", "Bearer " + token).asString
    val jsonValue2 = Json.parse(res.body)
    var content = jsonValue2.at("/content").asText()
    content = content.replaceAll("""<[^>]*>""", "")
    content = content.replaceAll("""&nbsp;""", " ")
    content = content.replaceAll("""\n""", " ")
    content = content.replaceAll("""[^a-zA-Z ]""", "")
    println(content)

    val word2vec = new Word2Vec()
    val input = sc.parallelize(content.split(" ")).map(line => line.split(" ").toSeq)
    println("parallelized ===============")

    val model = word2vec.fit(input)
    println("fitted ===============")

    val synonyms = model.findSynonyms("cup", 5)
    println("synonyms ===============")

    for((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }

    // Save and load model
//    model.save(sc, "myModelPath")
//    val sameModel = Word2VecModel.load(sc, "myModelPath")
  }

}
