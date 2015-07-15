//package raw
//
//import whisk.docker.{DockerContainer, DockerKit, DockerReadyChecker}
//
//trait DockerLDBService extends DockerKit {
//
//  def ldbPort = 5030
//
//  val ldbContainer = DockerContainer("raw/ldb")
//    .withPorts(5000 -> Some(ldbPort))
////    .withReadyChecker(DockerReadyChecker.LogLine(_.contains("Running on http://")))
//
//  abstract override def dockerContainers: List[DockerContainer] = ldbContainer :: super.dockerContainers
//}
