package raw.calculus

import raw._

object TestWorlds {

  import SymbolTable.ClassEntity

  def departments = {
    val address =
      RecordType(List(
        AttrType("street", StringType()),
        AttrType("zipcode", StringType())))

    val instructor =
      RecordType(List(
        AttrType("ssn", NumberType()),
        AttrType("name", StringType()),
        AttrType("address", address),
        AttrType("salary", NumberType()),
        AttrType("rank", StringType()),
        AttrType("degrees", SetType(StringType())),
        AttrType("dept", ClassType("Department")),
        AttrType("teaches", SetType(ClassType("Course")))))
    val instructors = SetType(instructor)

    val department =
      RecordType(List(
        AttrType("dno", NumberType()),
        AttrType("name", StringType()),
        AttrType("head", ClassType("Instructor")),
        AttrType("instructors", ClassType("Instructors")),
        AttrType("courses", ClassType("Courses"))))
    val departments = BagType(department)

    val course =
      RecordType(List(
        AttrType("code", StringType()),
        AttrType("name", StringType()),
        AttrType("offered_by", ClassType("Department")),
        AttrType("taught_by", ClassType("Instructor")),
        AttrType("is_prerequisite_for", ClassType("Courses")),
        AttrType("has_prerequisites", SetType(ClassType("Course")))))
    val courses = SetType(course)

    val userTypes = Map(
      "Instructor" -> instructor,
      "Instructors" -> instructors,
      "Department" -> department,
      "Departments" -> departments,
      "Course" -> course,
      "Courses" -> courses)

    val catalog = Map(
      "Departments" -> Source(departments, EmptyLocation)
    )

    new World(catalog, userTypes=userTypes)
  }

  def employees = {
    val children =
      ListType(
        RecordType(List(
          AttrType("age", NumberType()))))
    val manager =
      RecordType(List(
        AttrType("name", StringType()),
        AttrType("children", children)))
    val employees =
      SetType(
        RecordType(List(
          AttrType("children", children),
          AttrType("manager", manager))))

    val catalog = Map(
      "Employees" -> Source(employees, EmptyLocation)
    )

    new World(catalog)
  }

  def cern = {
    val events =
      ListType(
        RecordType(List(
          AttrType("RunNumber", NumberType()),
          AttrType("lbn", NumberType()),
          AttrType("muons",
            ListType(
              RecordType(List(
                AttrType("pt", NumberType()),
                AttrType("eta", NumberType()))))),
          AttrType("jets",
            ListType(
              RecordType(List(
                AttrType("pt", NumberType()),
                AttrType("eta", NumberType()))))))))

    val goodRuns =
      ListType(
        RecordType(List(
          AttrType("Run", NumberType()),
          AttrType("OK", BoolType()))))

    val catalog = Map(
      "Events" -> Source(events, EmptyLocation),
      "GoodRuns" -> Source(goodRuns, EmptyLocation)
    )

    new World(catalog)
  }

  def things = {
    val things =
      SetType(
        RecordType(List(
          AttrType("a", NumberType()),
          AttrType("b", NumberType()),
          AttrType("set_a",
            SetType(
              NumberType())),
          AttrType("set_b",
            SetType(
              NumberType())))))
              
    val catalog = Map(
      "things" -> Source(things, EmptyLocation)
    )

    new World(catalog)
  }

  def fines = {
    val speed_limits =
      ListType(
        RecordType(List(
          AttrType("location", StringType()),
          AttrType("min_speed", NumberType()),
          AttrType("max_speed", NumberType()))))

    val radar =
      ListType(
        RecordType(List(
          AttrType("person", StringType()),
          AttrType("speed", NumberType()),
        AttrType("location", StringType()))))

    val catalog = Map(
      "speed_limits" -> Source(speed_limits, EmptyLocation),
      "radar" -> Source(radar, EmptyLocation)
    )

    new World(catalog)
  }

}