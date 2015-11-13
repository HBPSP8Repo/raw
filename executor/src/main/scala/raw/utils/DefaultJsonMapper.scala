package raw.utils

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.StrictLogging

/**
  * Created by nuno on 11/12/15.
  */
/* Object mapper used to read/write any JSON received/sent by the rest server */
object DefaultJsonMapper extends StrictLogging {
  val mapper = {
    val om = new ObjectMapper()
    om.registerModule(DefaultScalaModule)
    om.configure(SerializationFeature.INDENT_OUTPUT, true)
    //    om.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
    om.setSerializationInclusion(Include.ALWAYS)
    om
  }
}
