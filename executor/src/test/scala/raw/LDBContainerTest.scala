///* TODO: Disabled for the time being. Currently the docker image with the OQL compilation server only needs to
// be present during compilation which is when we do code generation. In the future, we may switch to runtime
// code generation, then we will need docker at runtime.
// */
//
///*
// Note: docker-java is used DockerTestKit to communicate with the Docker daemon. This is causing problems
// with dependencies. docker-java depends on Jersey 2.X while Spark depends on Jersey 1.X. I was not able to
// get them to work together, I get this exception:
// java.lang.AbstractMethodError: javax.ws.rs.core.UriBuilder.resolveTemplate(Ljava/lang/String;Ljava/lang/Object;Z)Ljavax/ws/rs/core/UriBuilder;
//        at org.glassfish.jersey.client.JerseyWebTarget.resolveTemplate(JerseyWebTarget.java:246)
//        at org.glassfish.jersey.client.JerseyWebTarget.resolveTemplate(JerseyWebTarget.java:237)
//        at org.glassfish.jersey.client.JerseyWebTarget.resolveTemplate(JerseyWebTarget.java:59)
//        at com.github.dockerjava.jaxrs.StartContainerCmdExec.execute(StartContainerCmdExec.java:24)
//        at com.github.dockerjava.jaxrs.StartContainerCmdExec.execute(StartContainerCmdExec.java:13)
//        at com.github.dockerjava.jaxrs.AbstrDockerCmdExec.exec(AbstrDockerCmdExec.java:53)
//        at com.github.dockerjava.core.command.AbstrDockerCmd.exec(AbstrDockerCmd.java:29)
//        at com.github.dockerjava.core.command.StartContainerCmdImpl.exec(StartContainerCmdImpl.java:53)
//        at whisk.docker.DockerContainerOps$$anonfun$init$1$$anonfun$apply$9$$anonfun$apply$10.apply(DockerContainerOps.scala:84)
//        at whisk.docker.DockerContainerOps$$anonfun$init$1$$anonfun$apply$9$$anonfun$apply$10.apply(DockerContainerOps.scala:84)
//        at scala.concurrent.impl.Future$PromiseCompletingRunnable.liftedTree1$1(Future.scala:24)
//
//  UriBuilder is part of javax.ws.rs:javax.ws.rs-api, which is imported by Jersey. #resolveTemplate exists
//  only on version 2.0 of this library. I tried to explicitly import javax.ws.rs:javax.ws.rs-api:2.0 before
//  docker-java, so that it appears first on the classpath. But even in this case, docker-java is picking up
//  the UriBuilder class from ws.rs 1.X.
//
//  Excluding Jersey 1.X from the dependencies of Spark solves the problem for docker-java, but then Spark
//  no longer works because it relies on classes that were renamed between Jersey 1.X to Jersey 2.X (ServletContainer)
//
//  Possible solutions:
//  - hope that Spark moves to Jersey 2.0.
//  - Drop docker-java and instead call the Docker daemon REST interface directly or use the command line
//  - ???
//  */
//
//package raw
//
//import com.typesafe.scalalogging.StrictLogging
//import org.scalatest.{BeforeAndAfterAll, FunSuite}
//import whisk.docker.test.DockerTestKit
//
//class LDBContainerTest extends FunSuite with StrictLogging with BeforeAndAfterAll with DockerTestKit with DockerLDBService {
//  override def beforeAll(): Unit = {
//    println("Docker containers: " + super.dockerContainers)
//    println("Docker config: " + docker.config)
//    super.beforeAll()
//  }
//
//  test("start and stop docker container") {
//    println("Foo property: " + System.getProperty("foo"))
//
//    println(System.getProperties.toString.replace(", ", "\n"))
//    println("test code")
//  }
//}
