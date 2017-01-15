package de.tum.seba.morelikethis

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import spark.jobserver.{SparkJob, SparkJobInvalid, SparkJobValid, SparkJobValidation}

import scala.util.Try
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import play.libs.Json
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import scalaj.http.HttpResponse
import scalaj.http._

object VectorBuilderJob extends SparkJob {


  val baseUrl: String = "https://server.sociocortex.com/api/v1"
  val authUrl: String = baseUrl + "/jwt"
  val wsUrl: String = baseUrl + "/workspaces"
  val wsNwUrl: String = wsUrl + "/northwind"
  val entityTypNwUrl: String = wsNwUrl + "/entityTypes"
  val entitiesNwUrl: String = wsNwUrl + "/entities"
  val entityExNwUrl: String = baseUrl + "/entities/teatime"
  val prodNwUrl: String = baseUrl + "/entityTypes/nwproduct"

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

    val authResponse: HttpResponse[String] = Http(authUrl + "/").postData("{\n \"username\":\"prateek.bagrecha@outlook.com\",\n \"password\":\"Bakuman057\"\n}").header("content-type", "application/json").asString
    val authJson = Json.parse(authResponse.body)
    val token = authJson.at("/token").asText()

    // todo: loop all documents and get text content
    // todo: get all contents of a document
    // todo: get all contents of all child documents
    // todo: merge all contents (parent & child)
    // todo: fit the Model
    // todo: get all vectors
    // todo: add vectors to obtain document vector


    val res: HttpResponse[String] = Http("https://server.sociocortex.com/api/v1/entities/ose32apj5pa9").header("Authorization", "Bearer " + token).asString
    val jsonValue = Json.parse(res.body)

    var content = jsonValue.get("content").asText()

    //obtain id of the parent document for storing document vectors
    var entityId = jsonValue.get("id").asText()

    content = content.replaceAll("""<[^>]*>""", " ")
    content = content.replaceAll("""&nbsp;""", " ")
    content = content.replaceAll("""\n""", " ")
    //content = content.replaceAll("""[^a-zA-Z ]""", " ")
    println(content)

    val word2vec = new Word2Vec().setMinCount(0).setVectorSize(5)
    val input = sc.parallelize(content.split(" ")).map(line => line.split(" ").toSeq)
    println("parallelized ===============")

    val model = word2vec.fit(input)
    println("fitted ===============")

    //val synonyms = model.findSynonyms("fictitious", 5)
    val vectors = model.getVectors
    val vec1 = vectors.get("fictitious")
    val vec2 = vectors.get("imports")

    println("Vector 1: "+vec1.map(_.mkString(" ")).mkString("\n"))
    println("Vector 2: "+vec2.map(_.mkString(" ")).mkString("\n"))

    val a = Array (vec1, vec2).flatten
    val myres = a.transpose.map(_.sum)
    println("Result : "+myres.deep.mkString(" "))

   /* println("synonyms ===============")

    for((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }*/
    // Save and load model
   // model.save(sc, "myModelPath")
    //val sameModel = Word2VecModel.load(sc, "myModelPath")

  }

}
