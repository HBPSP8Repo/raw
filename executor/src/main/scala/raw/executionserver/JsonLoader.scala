package raw.executionserver

import java.io.InputStream
import java.nio.file.{Path, Files}

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.google.common.io.Resources
import com.typesafe.scalalogging.StrictLogging

import scala.reflect._
import scala.reflect.runtime.universe._

object JsonLoader extends StrictLogging {
  private[this] val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

//    def load[T](resource: String)(implicit ct: ClassTag[T]): List[T] = {
//      logger.info(s"Loading resource: $resource")
//      val is: InputStream = Resources.getResource(resource).openStream()
//      try {
//        val jp = new JsonFactory().createParser(is)
//        val reader: MappingIterator[T] = mapper.readValues(jp, ct.runtimeClass.asInstanceOf[Class[T]])
//        return JavaConversions.asScalaIterator(reader).toList
//      } finally {
//        is.close()
//      }
//    }
//
//  /* https://stackoverflow.com/questions/29127557/how-to-maintain-type-parameter-during-typetag-to-manifest-conversion */
//  def toManifest[T: TypeTag]: Manifest[T] = {
//    val t = typeTag[T]
//    val mirror = t.mirror
//    def toManifestRec(t: Type): Manifest[_] = {
//      val clazz = ClassTag[T](mirror.runtimeClass(t)).runtimeClass
//      if (t.typeArgs.length == 1) {
//        val arg = toManifestRec(t.typeArgs.head)
//        ManifestFactory.classType(clazz, arg)
//      } else if (t.typeArgs.length > 1) {
//        val args = t.typeArgs.map(x => toManifestRec(x))
//        ManifestFactory.classType(clazz, args.head, args.tail: _*)
//      } else {
//        ManifestFactory.classType(clazz)
//      }
//    }
//    toManifestRec(t.tpe).asInstanceOf[Manifest[T]]
//  }

//  def load[T: TypeTag](resource: String): T = {
//    logger.info(s"Loading resource: $resource")
//    val is: InputStream = Resources.getResource(resource).openStream()
//    try {
//      val jp = new JsonFactory().createParser(is)
//      val m = toManifest[T]
//      mapper.readValue(jp)(m)
//    } finally {
//      is.close()
//    }
//  }

  def load[T](resource: String)(implicit m:Manifest[T]): T = {
    logger.info(s"Loading resource: $resource")
    val is: InputStream = Resources.getResource(resource).openStream()
    try {
      val jp = new JsonFactory().createParser(is)
      mapper.readValue(jp)(m)
    } finally {
      is.close()
    }
  }

  def loadAbsolute[T](p: Path)(implicit m:Manifest[T]): T = {
    logger.info(s"Loading resource: $p")
    val is: InputStream = Files.newInputStream(p)
    try {
      val jp = new JsonFactory().createParser(is)
      mapper.readValue(jp)(m)
    } finally {
      is.close()
    }
  }
}

