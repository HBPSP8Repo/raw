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
        AttrType("ssn", IntType()),
        AttrType("name", StringType()),
        AttrType("address", address),
        AttrType("salary", IntType()),
        AttrType("rank", StringType()),
        AttrType("degrees", CollectionType(SetMonoid(), StringType())),
        AttrType("dept", ClassType("Department")),
        AttrType("teaches", CollectionType(SetMonoid(), ClassType("Course")))))
    val instructors = CollectionType(SetMonoid(), instructor)

    val department =
      RecordType(List(
        AttrType("dno", IntType()),
        AttrType("name", StringType()),
        AttrType("head", ClassType("Instructor")),
        AttrType("instructors", ClassType("Instructors")),
        AttrType("courses", ClassType("Courses"))))
    val departments = CollectionType(BagMonoid(), department)

    val course =
      RecordType(List(
        AttrType("code", StringType()),
        AttrType("name", StringType()),
        AttrType("offered_by", ClassType("Department")),
        AttrType("taught_by", ClassType("Instructor")),
        AttrType("is_prerequisite_for", ClassType("Courses")),
        AttrType("has_prerequisites", CollectionType(SetMonoid(), ClassType("Course")))))
    val courses = CollectionType(SetMonoid(), course)

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
      CollectionType(
        ListMonoid(),
        RecordType(List(
          AttrType("age", IntType()))))
    val manager =
      RecordType(List(
        AttrType("name", StringType()),
        AttrType("children", children)))
    val employees =
      CollectionType(
        SetMonoid(),
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
      CollectionType(ListMonoid(),
        RecordType(List(
          AttrType("RunNumber", IntType()),
          AttrType("lbn", IntType()),
          AttrType("muons",
            CollectionType(ListMonoid(),
              RecordType(List(
                AttrType("pt", FloatType()),
                AttrType("eta", FloatType()))))),
          AttrType("jets",
            CollectionType(ListMonoid(),
              RecordType(List(
                AttrType("pt", FloatType()),
                AttrType("eta", FloatType()))))))))

    val goodRuns =
      CollectionType(ListMonoid(),
        RecordType(List(
          AttrType("Run", IntType()),
          AttrType("OK", BoolType()))))

    val catalog = Map(
      "Events" -> Source(events, EmptyLocation),
      "GoodRuns" -> Source(goodRuns, EmptyLocation)
    )

    new World(catalog)
  }

  def things = {
    val things =
      CollectionType(SetMonoid(),
        RecordType(List(
          AttrType("a", IntType()),
          AttrType("b", IntType()),
          AttrType("set_a",
            CollectionType(SetMonoid(),
              FloatType())),
          AttrType("set_b",
            CollectionType(SetMonoid(),
              FloatType())))))
              
    val catalog = Map(
      "things" -> Source(things, EmptyLocation)
    )

    new World(catalog)
  }

}