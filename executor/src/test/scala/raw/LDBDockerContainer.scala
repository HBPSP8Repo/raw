package raw

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{BeforeAndAfterAll, Suite}
import raw.utils.DockerUtils

trait LDBDockerContainer extends BeforeAndAfterAll with StrictLogging {
  self: Suite =>

  override def beforeAll() {
    super.beforeAll()
    DockerUtils.setEnvironment()
    DockerUtils.startDocker()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    DockerUtils.stopDocker()
  }
}
