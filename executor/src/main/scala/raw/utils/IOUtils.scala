package raw.utils

import java.io.StringWriter

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{SerializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.StrictLogging
import raw.rest.ClientErrorException

/* Used to limit the size of the responses sent to the REST client.
 */
class BoundedStringWriter(val maxSize: Int) extends StringWriter(4096) {

  private[this] def checkLength(appendSize: Int) {
    if (super.getBuffer.length() + appendSize > maxSize) {
      throw new ClientErrorException("Output exceeds maximum size: " + maxSize)
    }
  }

  override def write(c: Int) {
    checkLength(1)
    super.write(c.toChar)
  }

  override def write(cbuf: Array[Char], off: Int, len: Int) {
    checkLength(len)
    super.write(cbuf, off, len)
  }

  override def write(str: String) {
    checkLength(str.length)
    super.write(str)
  }

  override def write(str: String, off: Int, len: Int) {
    checkLength(len)
    super.write(str, off, len)
  }
}


