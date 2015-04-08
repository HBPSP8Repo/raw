package raw.util

import raw._

object Worlds {
  val students = LocalFileLocation(
    ListType(RecordType(List(AttrType("name", StringType()), AttrType("birthYear", IntType()), AttrType("office", StringType()), AttrType("department", StringType())))),
    "src/test/data/smokeTest/students.csv",
    TextCsv(","))
  val profs = LocalFileLocation(
    ListType(RecordType(List(AttrType("name", StringType()), AttrType("office", StringType())))),
    "src/test/data/smokeTest/profs.csv",
    TextCsv(","))
  val departments = LocalFileLocation(
    ListType(RecordType(List(AttrType("name", StringType()), AttrType("discipline", StringType()), AttrType("prof", StringType())))),
    "src/test/data/smokeTest/departments.csv",
    TextCsv(","))
  val world: World = new World(Map(
    "students" -> students,
    "profs" -> profs,
    "departments" -> departments))
}