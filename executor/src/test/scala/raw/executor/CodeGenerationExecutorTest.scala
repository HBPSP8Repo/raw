package raw.executor

import java.nio.file.{Files, Paths}

import com.google.common.io.Resources
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSuite
import raw.{SharedSparkContext, AbstractRawTest, AbstractSparkTest}

class CodeGenerationExecutorTest extends AbstractRawTest with SharedSparkContext {
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

  ignore("scala") {
//    val p = Paths.get(Resources.getResource("data/patients/patients.schema.xml").toURI)
//    CodeGenerationExecutor.registerAccessPath("patients", Files.newBufferedReader(p), Paths.get(patientsFile))
//    val result = CodeGenerationExecutor.query(plan)
//    assert(result === "500")
  }

  ignore("spark") {
//    val p = Paths.get(Resources.getResource("data/patients/patients.schema.xml").toURI)
//    CodeGenerationExecutor.registerAccessPath("patients", Files.newBufferedReader(p), Paths.get(patientsFile), sc)
//    val result = CodeGenerationExecutor.query(plan)
//    assert(result === "500")
  }
}
