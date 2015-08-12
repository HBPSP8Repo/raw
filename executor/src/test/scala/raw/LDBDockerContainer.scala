package raw

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{BeforeAndAfterAll, Suite}
import raw.utils.DockerUtils

trait LDBDockerContainer extends BeforeAndAfterAll with StrictLogging {
  self: Suite =>

  override def beforeAll() {
    super.beforeAll()
    try {
      DockerUtils.setEnvironment()
      DockerUtils.startDocker()
    } catch {
      case ex: Exception =>
        super.afterAll()
        throw ex
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    DockerUtils.stopDocker()
  }
}
