package raw.executor

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/** TODO: Register files and other tests that should be done with an empty storage. */
class RestServerEmptyStorageTest extends FunSuite with RawRestService with StrictLogging with BeforeAndAfterAll {

  //  private[this] def stageResourceDir(ressource: String, toDir: String): Unit = {
  //    val downloadDir = testDir.resolve(toDir)
  //    RawUtils.cleanOrCreateDirectory(downloadDir)
  //    val studentsURL: Path = Paths.get(Resources.getResource(ressource).toURI)
  //    logger.info(s"Copying $ressource to $downloadDir")
  //    FileUtils.copyDirectory(studentsURL.toFile, downloadDir.toFile)
  //  }
  //
  //  ignore("JSON register && query") {
  //    stageResourceDir("data/patients", "downloaddata")
  //    val post = newRegisterPost("patients", "joedoe", "downloaddata")
  //    executeRequest(post)
  //
  //    val queryPost = newQueryPost(patientsPlan)
  //    executeRequest(queryPost)
  //  }
  //
  //  ignore("CSV register && query: students with header") {
  //    stageResourceDir("data/students", "downloaddata")
  //    val post = newRegisterPost("students", "joedoe", "downloaddata")
  //    executeRequest(post)
  //
  //    val queryPost = newQueryPost(studentsPlan)
  //    val resp = executeRequest(queryPost)
  //    assert(resp == "7")
  //  }
  //
  //  ignore("CSV register && query: students no header") {
  //    stageResourceDir("data/students_no_header", "downloaddata")
  //    val post = newRegisterPost("students", "joedoe", "downloaddata")
  //    executeRequest(post)
  //
  //    val queryPost = newQueryPost(studentsPlan)
  //    val resp = executeRequest(queryPost)
  //    assert(resp == "7")
  //  }
  //
  //  ignore("CSV register && query: brain_features_set header") {
  //    stageResourceDir("data/brain_feature_set", "downloaddata")
  //    val post = newRegisterPost("brain_feature_set", "joedoe", "downloaddata")
  //    executeRequest(post)
  //
  //    val queryPost = newQueryPost(brainFeatureSetPlan)
  //    val resp = executeRequest(queryPost)
  //    assert(resp == "1099")
  //  }

  //  ignore("RawServer") {
  //    val rawUser = "joedoe"
  //    stageResourceDir("data/brain_feature_set", "downloaddata")
  //    val storageManager = restServer.rawServer.storageManager
  //    storageManager.registerSchema("brain_feature_set", Paths.get("downloaddata"), rawUser)
  //    val scanner = storageManager.getScanner(rawUser, "brain_feature_set")
  //
  //    val schemas = storageManager.listUserSchemas(rawUser)
  //    logger.info("Found schemas: " + schemas.mkString(", "))
  //    val scanners: Seq[RawScanner[_]] = schemas.map(name => storageManager.getScanner(rawUser, name))
  //    val result = CodeGenerator.query(OQL, brainFeatureSetPlan, scanners)
  //    logger.info("Result: " + result)
  //  }


}
