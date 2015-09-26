package raw.executor

import java.nio.file.{Path, Paths}
import java.util

import com.google.common.io.Resources
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.http.NameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.{HttpPost, HttpUriRequest}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.message.BasicNameValuePair
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import raw.QueryLanguages.OQL
import raw.rest.RawRestServer
import raw.utils.RawUtils

import scala.concurrent.Await
import scala.concurrent.duration._

class RawRestServerTest extends FunSuite with StrictLogging with BeforeAndAfterAll {
  var restServer: RawRestServer = _
  var testDir: Path = _
  val httpclient = HttpClients.createDefault()

  override def beforeAll() = {
    testDir = RawUtils.getTemporaryDirectory("test-basedata")
    RawUtils.cleanOrCreateDirectory(testDir)
    restServer = new RawRestServer("scala", Some(testDir.toString))
    val serverUp = restServer.start()
    logger.info("Waiting for rest server to start")
    Await.ready(serverUp, Duration(2, SECONDS))
  }

  override def afterAll() = {
    restServer.stop()
    httpclient.close()
  }

  val patientsPlan = """
    Reduce(SumMonoid(),
       IntConst(1),
       BoolConst(true),
       Select(BoolConst(true),
              Scan(patients,
                   BagType(RecordType(Seq(AttrType(city,StringType()),
                                          AttrType(country,StringType()),
                                          AttrType(patient_id,StringType()),
                                          AttrType(year_of_birth,IntType()),
                                          AttrType(diagnosis,
                                                   ListType(RecordType(Seq(AttrType(diag_id,
                                                                                    StringType()),
                                                                           AttrType(code,
                                                                                    StringType()),
                                                                           AttrType(diag_date,
                                                                                    StringType()),
                                                                           AttrType(description,
                                                                                    StringType()),
                                                                           AttrType(patient_id,
                                                                                    StringType())),
                                                                       Diagnostic))),
                                          AttrType(gender,StringType())),
                                      Patient)))))
                     """

  val studentsPlan = """
    Reduce(SumMonoid(),
         IntConst(1),
         BoolConst(true),
         Select(BoolConst(true),
                Scan(students,
                     BagType(RecordType(Seq(AttrType(v1,StringType()),
                                            AttrType(v2,IntType()),
                                            AttrType(v3,StringType()),
                                            AttrType(v4,StringType())),
                                        students_1)))))
                     """

  val brainFeatureSetPlan =
    """
      |Reduce(SumMonoid(),
      |       IntConst(1),
      |       BoolConst(true),
      |       Select(BoolConst(true),
      |              Scan(brain_feature_set,
      |                   BagType(RecordType(Seq(AttrType(anonymization_method_version,StringType()),
      |                                          AttrType(description,StringType()),
      |                                          AttrType(brain_atlas_id,StringType()),
      |                                          AttrType(pipeline_version,StringType()),
      |                                          AttrType(anonymization_method,StringType()),
      |                                          AttrType(pipeline_name,StringType()),
      |                                          AttrType(extracted_from,StringType()),
      |                                          AttrType(exam_date,StringType()),
      |                                          AttrType(record_creation,StringType()),
      |                                          AttrType(patient_id,StringType()),
      |                                          AttrType(extraction_method,StringType()),
      |                                          AttrType(id,StringType()),
      |                                          AttrType(extraction_method_version,StringType())),
      |                                      brain_feature_set_1)))))
      |
    """.stripMargin

  private[this] def executeRequest(post: HttpUriRequest): String = {
    logger.info("Sending request: " + post)
    val response = httpclient.execute(post)
    val body = IOUtils.toString(response.getEntity.getContent)
    logger.info(s"Response: $body")
    body
  }

  private[this] def stageResourceDir(ressource: String, toDir: String): Unit = {
    val downloadDir = testDir.resolve(toDir)
    RawUtils.cleanOrCreateDirectory(downloadDir)
    val studentsURL: Path = Paths.get(Resources.getResource(ressource).toURI)
    logger.info(s"Copying $ressource to $downloadDir")
    FileUtils.copyDirectory(studentsURL.toFile, downloadDir.toFile)
  }

  def newRegisterPost(schemaName: String, rawUser: String, directory: String): HttpPost = {
    val post = new HttpPost("http://localhost:54321/register")
    val nvps = new util.ArrayList[NameValuePair]()
    nvps.add(new BasicNameValuePair("user", rawUser))
    nvps.add(new BasicNameValuePair("schemaName", schemaName))
    nvps.add(new BasicNameValuePair("dataDir", directory))
    post.setEntity(new UrlEncodedFormEntity(nvps))
    post
  }

  def newQueryPost(logicalPlan:String): HttpPost = {
    val queryPost = new HttpPost("http://localhost:54321/query")
    queryPost.setHeader("Raw-User", "joedoe")
    queryPost.setHeader("Raw-Query-Language", "qrawl")
    queryPost.setEntity(new StringEntity(logicalPlan))
    queryPost
  }

  test("JSON register && query") {
    stageResourceDir("data/patients", "downloaddata")
    val post = newRegisterPost("patients", "joedoe", "downloaddata")
    executeRequest(post)

    val queryPost = newQueryPost(patientsPlan)
    executeRequest(queryPost)
  }

  test("CSV register && query: students with header") {
    stageResourceDir("data/students", "downloaddata")
    val post = newRegisterPost("students", "joedoe", "downloaddata")
    executeRequest(post)

    val queryPost = newQueryPost(studentsPlan)
    val resp = executeRequest(queryPost)
    assert(resp == "7")
  }

  test("CSV register && query: students no header") {
    stageResourceDir("data/students_no_header", "downloaddata")
    val post = newRegisterPost("students", "joedoe", "downloaddata")
    executeRequest(post)

    val queryPost = newQueryPost(studentsPlan)
    val resp = executeRequest(queryPost)
    assert(resp == "7")
  }

  test("CSV register && query: brain_features_set header") {
    stageResourceDir("data/brain_feature_set", "downloaddata")
    val post = newRegisterPost("brain_feature_set", "joedoe", "downloaddata")
    executeRequest(post)

    val queryPost = newQueryPost(brainFeatureSetPlan)
    val resp = executeRequest(queryPost)
    assert(resp == "1099")
  }

  test("RawServer") {
    val rawUser = "joedoe"
    stageResourceDir("data/brain_feature_set", "downloaddata")
    val storageManager = restServer.rawServer.storageManager
    storageManager.registerSchema("brain_feature_set", "downloaddata", rawUser)
    val scanner = storageManager.getScanner(rawUser, "brain_feature_set")

    val schemas = storageManager.listUserSchemas(rawUser)
    logger.info("Found schemas: " + schemas.mkString(", "))
    val scanners: Seq[RawScanner[_]] = schemas.map(name => storageManager.getScanner(rawUser, name))
    val result = CodeGenerator.query(OQL, brainFeatureSetPlan, scanners)
    logger.info("Result: " + result)
  }
}
