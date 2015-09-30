package raw

import raw.calculus.Symbol

object TestWorlds {

  def linkedList = {
    val tipes = Map(
      Symbol {
        "Item"
      } -> RecordType(List(AttrType("value", IntType()), AttrType("next", UserType(Symbol("Item")))), Some("item"))
    )

    val sources = Map(
      "Items" -> CollectionType(ListMonoid(), UserType(Symbol("Item")))
    )

    new World(sources, tipes)
  }

  def publications = {
    val tipes = Map(
      Symbol("Publication") -> RecordType(List(AttrType("title", StringType()),
                                               AttrType("authors", CollectionType(BagMonoid(), StringType())),
                                               AttrType("affiliations", CollectionType(BagMonoid(), StringType())),
                                               AttrType("controlledterms", CollectionType(BagMonoid(), StringType()))), Some("publication")),
      Symbol("Author")      -> RecordType(List(AttrType("name", StringType()), AttrType("title", StringType()), AttrType("year", IntType())), Some("author"))
    )
    val sources = Map(
      "publications" -> CollectionType(BagMonoid(), UserType(Symbol("Publication"))),
      "authors" -> CollectionType(BagMonoid(), UserType(Symbol("Author")))
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
          AttrType("degrees", CollectionType(SetMonoid(), StringType())),
          AttrType("dept", UserType(Symbol("Department"))),
          AttrType("teaches", CollectionType(SetMonoid(), UserType(Symbol("Course"))))),
          Some("instructor")),
      Symbol("Instructors") ->
        CollectionType(SetMonoid(), UserType(Symbol("Instructor"))),
      Symbol("Department") ->
        RecordType(List(
          AttrType("dno", IntType()),
          AttrType("name", StringType()),
          AttrType("head", UserType(Symbol("Instructor"))),
          AttrType("instructors", UserType(Symbol("Instructors"))),
          AttrType("courses", UserType(Symbol("Courses")))),
          Some("department")),
      Symbol("Departments") ->
        CollectionType(BagMonoid(), UserType(Symbol("Department"))),
      Symbol("Course") ->
        RecordType(List(
          AttrType("code", StringType()),
          AttrType("name", StringType()),
          AttrType("offered_by", UserType(Symbol("Department"))),
          AttrType("taught_by", UserType(Symbol("Instructor"))),
          AttrType("is_prerequisite_for", UserType(Symbol("Courses"))),
          AttrType("has_prerequisites", CollectionType(SetMonoid(), UserType(Symbol("Course"))))),
          Some("course")),
      Symbol("Courses") ->
        CollectionType(SetMonoid(), UserType(Symbol("Course")))
    )

    val sources = Map(
      "Departments" -> UserType(Symbol("Departments"))
    )

    new World(sources, tipes)
  }

  def employees = {
    val children =
      CollectionType(ListMonoid(),
        RecordType(List(
          AttrType("age", IntType())),
          None))
    val manager =
      RecordType(List(
        AttrType("name", StringType()),
        AttrType("children", children)),
        None)
    val employees =
      CollectionType(SetMonoid(),
        RecordType(List(
          AttrType("dno", IntType()),
          AttrType("children", children),
          AttrType("manager", manager)),
          None))
    val departments =
      CollectionType(SetMonoid(),
        RecordType(List(AttrType("dno", IntType())), None))

    val sources = Map(
      "Employees" -> employees,
      "Departments" -> departments
    )

    new World(sources)
  }

  def A_B = {
    val A = CollectionType(ListMonoid(), IntType())
    val B = CollectionType(ListMonoid(), IntType())

    val sources = Map(
      "A" -> A,
      "B" -> B
    )

    new World(sources)
  }

  def transcripts = {
    val students =
      CollectionType(SetMonoid(),
        RecordType(List(
          AttrType("id", IntType()),
          AttrType("name", StringType())),
          None))

    val courses =
      CollectionType(SetMonoid(),
        RecordType(List(
          AttrType("cno", IntType()),
          AttrType("title", StringType())),
          None))

    val transcripts =
      CollectionType(SetMonoid(),
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
      CollectionType(ListMonoid(),
        RecordType(List(
          AttrType("RunNumber", IntType()),
          AttrType("lbn", IntType()),
          AttrType("muons",
            CollectionType(ListMonoid(),
              RecordType(List(
                AttrType("pt", FloatType()),
                AttrType("eta", FloatType())),
                None))),
          AttrType("jets",
            CollectionType(ListMonoid(),
              RecordType(List(
                AttrType("pt", FloatType()),
                AttrType("eta", FloatType())),
                None)))),
          None))

    val goodRuns =
      CollectionType(ListMonoid(),
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
      CollectionType(SetMonoid(),
        RecordType(List(
          AttrType("a", IntType()),
          AttrType("b", IntType()),
          AttrType("set_a",
            CollectionType(SetMonoid(),
              FloatType())),
          AttrType("set_b",
            CollectionType(SetMonoid(),
              FloatType()))),
          None))

    new World(sources = Map("things" -> things))
  }

  def fines = {
    val speed_limits =
      CollectionType(ListMonoid(),
        RecordType(List(
          AttrType("location", StringType()),
          AttrType("min_speed", IntType()),
          AttrType("max_speed", IntType())),
          None))

    val radar =
      CollectionType(ListMonoid(),
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
      CollectionType(ListMonoid(),
        RecordType(List(
          AttrType("name", StringType()),
          AttrType("birthYear", IntType()),
          AttrType("office", StringType()),
          AttrType("department", StringType())),
          None))

    val profs =
      CollectionType(ListMonoid(),
        RecordType(List(
          AttrType("name", StringType()),
          AttrType("office", StringType())),
          None))

    val departments =
      CollectionType(ListMonoid(),
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
      CollectionType(ListMonoid(),
        RecordType(List(
          AttrType("diagnostic_id", IntType()),
          AttrType("patient_id", IntType()),
          AttrType("diagnostic_code", StringType()),
          AttrType("diagnostic_type", StringType()),
          AttrType("diagnostic_date", StringType()),
          AttrType("hospital_id", StringType())),
          None))

    val diagnosis_codes =
      CollectionType(ListMonoid(),
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
      CollectionType(SetMonoid(),
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

  def simple = {
    new World(sources = Map(
      "unknown" -> CollectionType(SetMonoid(), TypeVariable()),
      "integers" -> CollectionType(SetMonoid(), IntType()),
      "floats" -> CollectionType(SetMonoid(), FloatType()),
      "booleans" -> CollectionType(SetMonoid(), BoolType()),
      "strings" -> CollectionType(SetMonoid(), StringType()),
      "records" -> CollectionType(SetMonoid(), RecordType(List(AttrType("i", IntType()), AttrType("f", FloatType())), None))))
  }

  def unknown = {
    new World(sources = Map(
      "unknownvalue" -> TypeVariable(),
      "unknown" -> CollectionType(SetMonoid(), TypeVariable()),
      "unknownrecords" -> CollectionType(SetMonoid(), RecordType(List(AttrType("dead", TypeVariable()), AttrType("alive", TypeVariable())), None))))
  }

  def professors_students = {
    val tipes = Map(
      Symbol("student") -> RecordType(List(AttrType("name", StringType()), AttrType("age", IntType())), Some("Student")),
      Symbol("students") -> CollectionType(ListMonoid(), UserType(Symbol("student"))),
      Symbol("professor") -> RecordType(List(AttrType("name", StringType()), AttrType("age", IntType())), Some("Professors")),
      Symbol("professors") -> CollectionType(ListMonoid(), UserType(Symbol("professor"))))

    val sources = Map(
      "students" -> UserType(Symbol("students")),
      "professors" -> UserType(Symbol("professors")))

    new World(sources, tipes)
  }

  def empty = new World()

  def options = new World(sources = {
    val i = IntType()
    val oi = IntType();
    oi.nullable = true
    val li = CollectionType(ListMonoid(), i)
    val loi = CollectionType(ListMonoid(), oi)
    val oli = CollectionType(ListMonoid(), i);
    oli.nullable = true
    val oloi = CollectionType(ListMonoid(), oi);
    oloi.nullable = true
    Map(
      "LI" -> li,
      "LOI" -> loi,
      "OLI" -> oli,
      "OLOI" -> oloi,
      "records" -> CollectionType(ListMonoid(), RecordType(List(AttrType("OI", oi), AttrType("LOI", loi),
        AttrType("I", IntType()), AttrType("LI", li)), None))
    )
  }
  )

  def authors_publications = {
    val tipes = Map(
      Symbol("author") -> RecordType(List(AttrType("name", StringType()), AttrType("title", StringType()), AttrType("year", IntType())), Some("author")),
      Symbol("publication") -> RecordType(List(AttrType("title", StringType()), AttrType("authors", CollectionType(BagMonoid(), StringType())), AttrType("affiliations", CollectionType(BagMonoid(), StringType())), AttrType("controlledterms", CollectionType(BagMonoid(), StringType()))), Some("publication")),
      Symbol("authors") -> CollectionType(BagMonoid(), UserType(Symbol("author"))),
      Symbol("publications") -> CollectionType(BagMonoid(), UserType(Symbol("publication"))))

    val sources = Map(
      "authors" -> UserType(Symbol("authors")),
      "publications" -> UserType(Symbol("publications")))

    new World(sources, tipes)
  }
}