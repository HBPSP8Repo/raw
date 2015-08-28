package raw.executor

import java.nio.file.{Files, Paths}

import com.google.common.io.Resources
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSuite
import raw.executionserver.RawServer

class SchemaParserTest extends FunSuite with StrictLogging {
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
  val patientsFile = "C:\\cygwin64\\home\\nuno\\code\\raw\\executor\\src\\main\\resources\\data\\patients\\patients.json"

  test("parse schema") {
    val p = Paths.get(Resources.getResource("rawschema.xml").toURI)
    RawServer.registerSchema("patients", Files.newBufferedReader(p), patientsFile)

    val result = RawServer.query(plan, List("patients"))
    logger.info("Result: " + result)
  }
}
