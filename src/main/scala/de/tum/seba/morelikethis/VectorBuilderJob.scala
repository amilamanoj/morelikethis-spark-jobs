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
    var docVectorMap = scala.collection.mutable.Map.empty[String, Array[Float]]

    println("Fetching all documents of workspace...")
    var startTime: Long = System.currentTimeMillis

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
      val jsonValue2 = Json.parse(res.body)
      var content = jsonValue2.at("/content").asText()
      content = content.replaceAll("""<[^>]*>""", "")
      content = content.replaceAll("""&nbsp;""", " ")
      content = content.replaceAll("""\n""", " ")
      content = content.replaceAll("""[^a-zA-Z ]""", "")
      if (!content.isEmpty) {
        print(elementId)
        documentMap(elementId) = content
        wholeContent = wholeContent.concat(content)
      }
    }
    val fetchTime = System.currentTimeMillis - startTime
    println()
    println("Fetched " + documentMap.size + " documents of workspace in milliseconds: " + fetchTime)

    println("Starting model fitting...")
    startTime = System.currentTimeMillis

    val word2vec = new Word2Vec().setMinCount(0).setVectorSize(100)
    val input = sc.parallelize(wholeContent.split(" ")).map(line => line.split(" ").toSeq)

    val model = word2vec.fit(input)
    val map = model.getVectors

    val fitTime = System.currentTimeMillis - startTime
    println("Fitted model in milliseconds: " + fitTime)

    println("Calculating document vectors: ")
    startTime = System.currentTimeMillis

    documentMap.foreach {
      keyVal =>
        println("  Calculating vector for document: " + keyVal._1)
        var docWords = keyVal._2.split(" +")
        var vecArray2d = new Array[Array[Float]](docWords.length)


        for (i <- docWords.indices) {
          var word = docWords(i)
          var wordVec = map.get(word)
          if (wordVec.nonEmpty) {
            vecArray2d(i) = wordVec.get
          } else {
            vecArray2d(i) = Array.fill[Float](100)(0)
          }
        }
        var docVectorArr = vecArray2d.transpose.map(_.sum)
        docVectorMap(keyVal._1) = docVectorArr
    }

    val docVecTime = System.currentTimeMillis - startTime
    println("Finished calculating " + documentMap.size + " document vectors in milliseconds: " + docVecTime)


    // Save and load model
//    model.save(sc, "myModelPath")
//    val sameModel = Word2VecModel.load(sc, "myModelPath")
  }

}
