package raw.util

import raw._

object Worlds {
  val students = ListType(
    RecordType(
      List(
        AttrType("name", StringType()),
        AttrType("birthYear", IntType()),
        AttrType("office", StringType()),
        AttrType("department", StringType())),
    None))

  val profs = ListType(
    RecordType(
      List(
        AttrType("name", StringType()),
        AttrType("office", StringType())),
    None))

  val departments = ListType(
    RecordType(
      List(
        AttrType("name", StringType()),
        AttrType("discipline", StringType()),
        AttrType("prof", StringType())),
    None))

  val world: World = new World(Map(
    "students" -> students,
    "profs" -> profs,
    "departments" -> departments))
}