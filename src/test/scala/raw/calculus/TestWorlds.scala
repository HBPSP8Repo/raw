package raw
package calculus

import raw.util.MemoryLocation

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

    val dataSources = Map(
      "Departments" -> EmptyDataSource(UserType("Departments"))
    )

    new World(userTypes, dataSources)
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

    val dataSources = Map(
      "Employees" -> EmptyDataSource(employees)
    )

    new World(dataSources=dataSources)
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

    val dataSources = Map(
      "Events" -> EmptyDataSource(events),
      "GoodRuns" -> EmptyDataSource(goodRuns)
    )

    new World(dataSources=dataSources)
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

    new World(dataSources=Map("things" -> EmptyDataSource(things)))
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

    val dataSources = Map(
      "speed_limits" -> EmptyDataSource(speed_limits),
      "radar" -> EmptyDataSource(radar)
    )

    new World(dataSources=dataSources)
  }

  def empty = new World()

}