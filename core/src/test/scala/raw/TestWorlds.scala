package raw

object TestWorlds {

  def departments = {
    val address =
      RecordType(List(
        AttrType("street", StringType()),
        AttrType("zipcode", StringType())))

    val instructor =
      RecordType(List(
        AttrType("ssn", IntType()),
        AttrType("name", StringType()),
        AttrType("address", address),
        AttrType("salary", IntType()),
        AttrType("rank", StringType()),
        AttrType("degrees", SetType(StringType())),
        AttrType("dept", UserType("Department")),
        AttrType("teaches", SetType(UserType("Course")))))
    val instructors = SetType(instructor)

    val department =
      RecordType(List(
        AttrType("dno", IntType()),
        AttrType("name", StringType()),
        AttrType("head", UserType("Instructor")),
        AttrType("instructors", UserType("Instructors")),
        AttrType("courses", UserType("Courses"))))
    val departments = BagType(department)

    val course =
      RecordType(List(
        AttrType("code", StringType()),
        AttrType("name", StringType()),
        AttrType("offered_by", UserType("Department")),
        AttrType("taught_by", UserType("Instructor")),
        AttrType("is_prerequisite_for", UserType("Courses")),
        AttrType("has_prerequisites", SetType(UserType("Course")))))
    val courses = SetType(course)

    val userTypes = Map(
      "Instructor" -> instructor,
      "Instructors" -> instructors,
      "Department" -> department,
      "Departments" -> departments,
      "Course" -> course,
      "Courses" -> courses
    )

    val sources = Map(
      "Departments" -> UserType("Departments")
    )

    new World(sources, userTypes)
  }

  def employees = {
    val children =
      ListType(
        RecordType(List(
          AttrType("age", IntType()))))
    val manager =
      RecordType(List(
        AttrType("name", StringType()),
        AttrType("children", children)))
    val employees =
      SetType(
        RecordType(List(
          AttrType("children", children),
          AttrType("manager", manager))))

    val sources = Map(
      "Employees" -> employees
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
                AttrType("eta", FloatType()))))),
          AttrType("jets",
            ListType(
              RecordType(List(
                AttrType("pt", FloatType()),
                AttrType("eta", FloatType()))))))))

    val goodRuns =
      ListType(
        RecordType(List(
          AttrType("Run", IntType()),
          AttrType("OK", BoolType()))))

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
              FloatType())))))

    new World(sources=Map("things" -> things))
  }

  def fines = {
    val speed_limits =
      ListType(
        RecordType(List(
          AttrType("location", StringType()),
          AttrType("min_speed", IntType()),
          AttrType("max_speed", IntType()))))

    val radar =
      ListType(
        RecordType(List(
          AttrType("person", StringType()),
          AttrType("speed", IntType()),
        AttrType("location", StringType()))))

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
        AttrType("department", StringType()))))

    val profs =
      ListType(
        RecordType(List(
          AttrType("name", StringType()),
          AttrType("office", StringType()))))

    val departments =
      ListType(
        RecordType(List(
          AttrType("name", StringType()),
          AttrType("discipline", StringType()),
          AttrType("prof", StringType()))))

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
          AttrType("hospital_id", StringType()))))

    val diagnosis_codes =
      ListType(
        RecordType(List(
          AttrType("diagnostic_code", StringType()),
          AttrType("valid_for_coding", StringType()),
          AttrType("description", StringType()))))

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
          AttrType("_4", IntType()))))

    val sources = Map(
      "set_of_tuples" -> tuples
    )
    new World(sources)
  }



  def empty = new World()

}