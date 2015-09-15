package raw.executor

import java.io.ByteArrayOutputStream
import java.nio.file.Paths

import com.google.common.io.Resources
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{StringEntity, FileEntity}
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

  val plan = """
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

  val patientsURI = "file:///C:/cygwin64/home/nuno/code/raw/executor/src/main/resources/data/patients/patients.json"
  //  val patientsFile = "C:\\cygwin64\\home\\nuno\\code\\raw\\executor\\src\\main\\resources\\data\\patients\\patients.json"

  private[this] def registerSchema(): Unit = {
    val post = new HttpPost("http://localhost:54321/register")
    post.setHeader("Content-Type", "application/xml")
    post.setHeader("Raw-Schema-Name", "patients")
    post.setHeader("Raw-File", patientsURI)
    post.setHeader("Raw-User", "joedoe")
    val p = Paths.get(Resources.getResource("rawschema.xml").toURI)
    post.setEntity(new FileEntity(p.toFile))
    logger.info("Sending request: " + post)
    val response = httpclient.execute(post)
    val body = IOUtils.toString(response.getEntity().getContent())
    logger.info(s"Response: $body")
  }

  def query(): Unit = {
    val post = new HttpPost("http://localhost:54321/query")
    post.setHeader("Raw-User", "joedoe")
    post.setEntity(new StringEntity(plan))
    val response = httpclient.execute(post)
    val body = IOUtils.toString(response.getEntity().getContent())
    logger.info(s"Response: $body")
  }

  test("parse schema") {
    registerSchema()
    query()
  }
}
