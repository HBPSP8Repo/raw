package raw.executor

import java.net.URL
import java.nio.file.Paths

import com.google.common.io.Resources
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.{HttpPost, HttpUriRequest}
import org.apache.http.entity.{FileEntity, StringEntity}
import org.apache.http.impl.client.HttpClients
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import raw.rest.RawRestServer

class RawRestServerTest extends FunSuite with StrictLogging with BeforeAndAfterAll {

  var restServer = new RawRestServer("scala")
  val httpclient = HttpClients.createDefault();

  override def beforeAll() = {
    restServer.start()
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

  def executeRequest(post: HttpUriRequest): String = {
    logger.info("Sending request: " + post)
    val response = httpclient.execute(post)
    val body = IOUtils.toString(response.getEntity().getContent())
    logger.info(s"Response: $body")
    body
  }

  def newRegisterPost(schemaName: String, file: URL, rawUser: String, bodyResource: String): HttpPost = {
    val post = new HttpPost("http://localhost:54321/register")
    post.setHeader("Content-Type", "application/xml")
    post.setHeader("Raw-Schema-Name", schemaName)
    post.setHeader("Raw-File", file.toString)
    post.setHeader("Raw-User", rawUser)
    val p = Paths.get(Resources.getResource(bodyResource).toURI)
    post.setEntity(new FileEntity(p.toFile))
    post
  }

  test("JSON register && query") {
    val patientsURL: URL = Resources.getResource("data/patients/patients.json")
    val registerPost = newRegisterPost("patients", patientsURL, "joedoe", "data/patients/patients.schema.xml")
    executeRequest(registerPost)

    val queryPost = new HttpPost("http://localhost:54321/query")
    queryPost.setHeader("Raw-User", "joedoe")
    queryPost.setEntity(new StringEntity(patientsPlan))
    executeRequest(queryPost)
  }

  test("CSV register && query") {
    val studentsURL: URL = Resources.getResource("data/students.csv")
    val post = newRegisterPost("students", studentsURL, "joedoe", "data/students.schema.xml")
    executeRequest(post)

    val queryPost = new HttpPost("http://localhost:54321/query")
    queryPost.setHeader("Raw-User", "joedoe")
    queryPost.setEntity(new StringEntity(studentsPlan))
    executeRequest(queryPost)
  }
}
