package de.tum.seba.morelikethis

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import spark.jobserver.{SparkJob, SparkJobInvalid, SparkJobValid, SparkJobValidation}

import scala.util.Try
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import play.libs.Json

import scala.collection.mutable
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
    var docSimilarityMap = scala.collection.mutable.Map.empty[String, scala.collection.mutable.Map[String, Double]]

    println("Fetching all documents of workspace...")
    var startTime: Long = System.currentTimeMillis

    val authResponse: HttpResponse[String] = Http(authUrl).postData("{\n \"username\":\"ga89tok@mytum.de\",\n \"password\":\"123123\"\n}").header("content-type", "application/json").asString
    val authJson = Json.parse(authResponse.body)
    val token = authJson.at("/token").asText()

    val res: HttpResponse[String] = Http(entitiesNwUrl).header("Authorization", "Bearer " + token).asString
    val jsonValue2 = Json.parse(res.body)
    val it = jsonValue2.elements()

    var wholeContent = ""
    var reqCount = 0
    while (it.hasNext) {
      reqCount = reqCount + 1
      val element = it.next()
      val elementId = element.at("/id").asText()
      val href = element.at("/href").asText()

      val res: HttpResponse[String] = Http(href).header("Authorization", "Bearer " + token).asString
      //    println(res.body)
      val jsonValue2 = Json.parse(res.body)
      var content = jsonValue2.at("/content").asText()
      content = content.replaceAll("""<[^>]*>""", "")
      content = content.replaceAll("""&nbsp;""", " ")
      content = content.replaceAll("""\n""", " ")
      content = content.replaceAll("""[^a-zA-Z ]""", "")
      if (!content.trim.isEmpty) {
        print(".")
        //        print(elementId)
        //        println(href)
        //        println(content)
        documentMap(elementId) = content
        wholeContent = wholeContent.concat(content)
      }
    }
    val fetchTime = System.currentTimeMillis - startTime
    println()
    println("Fetched " + documentMap.size + " non-empty documents out of " + reqCount + " total in milliseconds: " + fetchTime)
    //        println(documentMap)

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
        //        println("  Calculating vector for document: " + keyVal._1)
        print(".")
        var docWords = keyVal._2.split(" +")
        var vecArray2d = new Array[Array[Float]](docWords.length)


        for (i <- docWords.indices) {
          //          println(s"$i is ${docWords(i)}")
          var word = docWords(i)
          var wordVec = map.get(word)
          if (wordVec.nonEmpty) {
            vecArray2d(i) = wordVec.get
          } else {
            vecArray2d(i) = Array.fill[Float](100)(0)
          }
        }
        var docVectorArr = vecArray2d.transpose.map(_.sum)
        //        println(docVectorArr.deep.mkString(" "))
        docVectorMap(keyVal._1) = docVectorArr
    }

    val docVecTime = System.currentTimeMillis - startTime
    println()
    println("Finished calculating " + documentMap.size + " document vectors in milliseconds: " + docVecTime)

    println("Calculating similarity for " + docVectorMap.size * docVectorMap.size + " entries")
    startTime = System.currentTimeMillis

    docVectorMap.foreach {
      keyValX =>
        val similarityMapofX = scala.collection.mutable.Map.empty[String, Double]
        docVectorMap.foreach {
          keyValY =>
            val similarity = cosineSimilarity(keyValX._2, keyValY._2)
            //            println(keyValX._1 + "-" + keyValY._1 + ": " + similarity)
            similarityMapofX(keyValY._1) = similarity
        }
        docSimilarityMap(keyValX._1) = mutable.ListMap(similarityMapofX.toList.sortBy{_._2}:_*)
    }

    val similarityTime = System.currentTimeMillis - startTime
    println("Finished calculating similarity for " + docVectorMap.size * docVectorMap.size + " entries in milliseconds: " + similarityTime)
    //    println()
    //    println(docSimilarityMap)


    // Save and load model
//    model.save(sc, "myModelPath")
//    val sameModel = Word2VecModel.load(sc, "myModelPath")
  }

  /*
* This method takes 2 equal length arrays of integers
* It returns a double representing similarity of the 2 arrays
* 0.9925 would be 99.25% similar
* (x dot y)/||X|| ||Y||
*/
  def cosineSimilarity(x: Array[Float], y: Array[Float]): Double = {
    require(x.size == y.size)
    dotProduct(x, y)/(magnitude(x) * magnitude(y))
  }
  /*
   * Return the dot product of the 2 arrays
   * e.g. (a[0]*b[0])+(a[1]*a[2])
   */
  def dotProduct(x: Array[Float], y: Array[Float]): Float = {
    (for((a, b) <- x zip y) yield a * b) sum
  }
  /*
   * Return the magnitude of an array
   * We multiply each element, sum it, then square root the result.
   */
  def magnitude(x: Array[Float]): Double = {
    math.sqrt(x map(i => i*i) sum)
  }

}
