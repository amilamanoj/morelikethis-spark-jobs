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

  val authUrl: String = "https://server.sociocortex.com/api/v1/jwt"
  val entitiesNwUrl: String = "https://server.sociocortex.com/api/v1/workspaces/5f7u30lbgu35/entities"
  val wsUrl: String = "https://server.sociocortex.com/api/v1/workspaces"

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

    var documentMap = scala.collection.mutable.Map.empty[String, String]

    val authResponse: HttpResponse[String] = Http(authUrl).postData("{\n \"username\":\"ga89tok@mytum.de\",\n \"password\":\"123123\"\n}").header("content-type", "application/json").asString
    val authJson = Json.parse(authResponse.body)
    val token = authJson.at("/token").asText()

    val res: HttpResponse[String] = Http(entitiesNwUrl).header("Authorization", "Bearer " + token).asString
    val jsonValue2 = Json.parse(res.body)
    val it = jsonValue2.elements()

    var wholeContent = ""
    while (it.hasNext) {
      val element = it.next()
      val elementId = element.at("/id").asText()
      val href = element.at("/href").asText()

      val res: HttpResponse[String] = Http(href).header("Authorization", "Bearer " + token).asString
      //      println(res.body)
      val jsonValue2 = Json.parse(res.body)
      var content = jsonValue2.at("/content").asText()
      content = content.replaceAll("""<[^>]*>""", "")
      content = content.replaceAll("""&nbsp;""", " ")
      content = content.replaceAll("""\n""", " ")
      content = content.replaceAll("""[^a-zA-Z ]""", "")
      if (!content.isEmpty) {
        println(elementId)
        //        println(href)
        //        println(content)
        documentMap(elementId) = content
        wholeContent = wholeContent.concat(content)
      }
    }
    //    println(documentMap)

    documentMap.foreach {
      keyVal => println(keyVal._1 + "calculating doc vector")
        println(keyVal._2.split(" +")) }

    val conf = new SparkConf().setAppName("Word2VecExample").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val word2vec = new Word2Vec().setMinCount(0).setVectorSize(100)
    val input = sc.parallelize(wholeContent.split(" ")).map(line => line.split(" ").toSeq)
    println("parallelized ===============")

    val model = word2vec.fit(input)
    println("fitted ===============")

    val map = model.getVectors
    val vec1 = map.get("relevance")
    val vec2 = map.get("project")
    println(vec1.map(_.mkString(" ")).mkString("\n"))
    println(vec2.map(_.mkString(" ")).mkString("\n"))
    var a = Array(vec1, vec2).flatten
    var myres = a.transpose.map(_.sum)
    println(myres.deep.mkString(" "))



    // Save and load model
//    model.save(sc, "myModelPath")
//    val sameModel = Word2VecModel.load(sc, "myModelPath")
  }

}
