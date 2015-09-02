package raw

import calculus.Symbol

object TestWorlds {

  def linkedList = {
    val tipes = Map(
      Symbol{"Item"} -> RecordType(List(AttrType("value", IntType()), AttrType("next", UserType(Symbol("Item")))), Some("item"))
    )

    val sources = Map(
      "Items" -> ListType(UserType(Symbol("Item")))
    )

    new World(sources, tipes)
  }

  def departments = {

    val tipes = Map(
      Symbol("Instructor") ->
        RecordType(List(
          AttrType("ssn", IntType()),
          AttrType("name", StringType()),
          AttrType("address", RecordType(List(
            AttrType("street", StringType()),
            AttrType("zipcode", StringType())),
            None)),
          AttrType("salary", IntType()),
          AttrType("rank", StringType()),
          AttrType("degrees", SetType(StringType())),
          AttrType("dept", UserType(Symbol("Department"))),
          AttrType("teaches", SetType(UserType(Symbol("Course"))))),
          Some("instructor")),
      Symbol("Instructors") ->
        SetType(UserType(Symbol("Instructor"))),
      Symbol("Department") ->
        RecordType(List(
          AttrType("dno", IntType()),
          AttrType("name", StringType()),
          AttrType("head", UserType(Symbol("Instructor"))),
          AttrType("instructors", UserType(Symbol("Instructors"))),
          AttrType("courses", UserType(Symbol("Courses")))),
          Some("department")),
      Symbol("Departments") ->
        BagType(UserType(Symbol("Department"))),
      Symbol("Course") ->
        RecordType(List(
          AttrType("code", StringType()),
          AttrType("name", StringType()),
          AttrType("offered_by", UserType(Symbol("Department"))),
          AttrType("taught_by", UserType(Symbol("Instructor"))),
          AttrType("is_prerequisite_for", UserType(Symbol("Courses"))),
          AttrType("has_prerequisites", SetType(UserType(Symbol("Course"))))),
          Some("course")),
      Symbol("Courses") ->
        SetType(UserType(Symbol("Course")))
    )

    val sources = Map(
      "Departments" -> UserType(Symbol("Departments"))
    )

    new World(sources, tipes)
  }

  def employees = {
    val children =
      ListType(
        RecordType(List(
          AttrType("age", IntType())),
          None))
    val manager =
      RecordType(List(
        AttrType("name", StringType()),
        AttrType("children", children)),
        None)
    val employees =
      SetType(
        RecordType(List(
          AttrType("dno", IntType()),
          AttrType("children", children),
          AttrType("manager", manager)),
          None))
    val departments =
      SetType(
        RecordType(List(AttrType("dno", IntType())), None))

    val sources = Map(
      "Employees" -> employees,
      "Departments" -> departments
    )

    new World(sources)
  }

  def A_B = {
    val A = ListType(IntType())
    val B = ListType(IntType())

    val sources = Map(
      "A" -> A,
      "B" -> B
    )

    new World(sources)
  }

  def transcripts = {
    val students =
      SetType(
        RecordType(List(
          AttrType("id", IntType()),
          AttrType("name", StringType())),
          None))

    val courses =
      SetType(
        RecordType(List(
          AttrType("cno", IntType()),
          AttrType("title", StringType())),
          None))

    val transcripts =
      SetType(
        RecordType(List(
          AttrType("id", IntType()),
          AttrType("cno", IntType())),
          None))

    val sources = Map(
      "Students" -> students,
      "Courses" -> courses,
      "Transcripts" -> transcripts
    )

    new World(sources)
  }

  def cern = {
    val events =
      ListType(
        RecordType(List(
          AttrType("RunNumber", IntType()),
          AttrType("lbn", IntType()),
          AttrType("muons",
            ListType(
              RecordType(List(
                AttrType("pt", FloatType()),
                AttrType("eta", FloatType())),
                None))),
          AttrType("jets",
            ListType(
              RecordType(List(
                AttrType("pt", FloatType()),
                AttrType("eta", FloatType())),
                None)))),
          None))

    val goodRuns =
      ListType(
        RecordType(List(
          AttrType("Run", IntType()),
          AttrType("OK", BoolType())),
          None))

    val sources = Map(
      "Events" -> events,
      "GoodRuns" -> goodRuns
    )

    new World(sources)
  }

  def things = {
    val things =
      SetType(
        RecordType(List(
          AttrType("a", IntType()),
          AttrType("b", IntType()),
          AttrType("set_a",
            SetType(
              FloatType())),
          AttrType("set_b",
            SetType(
              FloatType()))),
          None))

    new World(sources = Map("things" -> things))
  }

  def fines = {
    val speed_limits =
      ListType(
        RecordType(List(
          AttrType("location", StringType()),
          AttrType("min_speed", IntType()),
          AttrType("max_speed", IntType())),
          None))

    val radar =
      ListType(
        RecordType(List(
          AttrType("person", StringType()),
          AttrType("speed", IntType()),
          AttrType("location", StringType())),
          None))

    val sources = Map(
      "speed_limits" -> speed_limits,
      "radar" -> radar
    )

    new World(sources)
  }

  def school = {
    val students =
      ListType(
        RecordType(List(
          AttrType("name", StringType()),
          AttrType("birthYear", IntType()),
          AttrType("office", StringType()),
          AttrType("department", StringType())),
          None))

    val profs =
      ListType(
        RecordType(List(
          AttrType("name", StringType()),
          AttrType("office", StringType())),
          None))

    val departments =
      ListType(
        RecordType(List(
          AttrType("name", StringType()),
          AttrType("discipline", StringType()),
          AttrType("prof", StringType())),
          None))

    val sources = Map(
      "students" -> students,
      "profs" -> profs,
      "departments" -> departments
    )

    new World(sources)
  }

  def chuv_diagnosis = {
    val diagnosis =
      ListType(
        RecordType(List(
          AttrType("diagnostic_id", IntType()),
          AttrType("patient_id", IntType()),
          AttrType("diagnostic_code", StringType()),
          AttrType("diagnostic_type", StringType()),
          AttrType("diagnostic_date", StringType()),
          AttrType("hospital_id", StringType())),
          None))

    val diagnosis_codes =
      ListType(
        RecordType(List(
          AttrType("diagnostic_code", StringType()),
          AttrType("valid_for_coding", StringType()),
          AttrType("description", StringType())),
          None))

    val sources = Map(
      "diagnosis" -> diagnosis,
      "diagnosis_codes" -> diagnosis_codes)

    new World(sources)
  }

  def set_of_tuples = {
    val tuples =
      SetType(
        RecordType(List(
          AttrType("_1", IntType()),
          AttrType("_2", IntType()),
          AttrType("_3", IntType()),
          AttrType("_4", IntType())),
          None))

    val sources = Map(
      "set_of_tuples" -> tuples
    )
    new World(sources)
  }

  def empty = new World()

}