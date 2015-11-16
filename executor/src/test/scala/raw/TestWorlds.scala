package raw

import raw.calculus.Symbol

object TestWorlds {

  def linked_list = {
    val tipes = Map(
      Symbol {
        "Item"
      } -> RecordType(Attributes(List(AttrType("value", IntType()), AttrType("next", UserType(Symbol("Item"))))))
    )

    val sources = Map(
      "Items" -> CollectionType(ListMonoid(), UserType(Symbol("Item")))
    )

    new World(sources, tipes)
  }

  def publications = {
    val tipes = Map(
      Symbol("Publication") -> RecordType(Attributes(List(AttrType("title", StringType()),
                                               AttrType("authors", CollectionType(BagMonoid(), StringType())),
                                               AttrType("affiliations", CollectionType(BagMonoid(), StringType())),
                                               AttrType("controlledterms", CollectionType(BagMonoid(), StringType()))))),
      Symbol("Author")      -> RecordType(Attributes(List(AttrType("name", StringType()), AttrType("title", StringType()), AttrType("year", IntType()))))
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
        RecordType(Attributes(List(
          AttrType("ssn", IntType()),
          AttrType("name", StringType()),
          AttrType("address", RecordType(Attributes(List(
            AttrType("street", StringType()),
            AttrType("zipcode", StringType()))))),
          AttrType("salary", IntType()),
          AttrType("rank", StringType()),
          AttrType("degrees", CollectionType(SetMonoid(), StringType())),
          AttrType("dept", UserType(Symbol("Department"))),
          AttrType("teaches", CollectionType(SetMonoid(), UserType(Symbol("Course"))))))),
      Symbol("Instructors") ->
        CollectionType(SetMonoid(), UserType(Symbol("Instructor"))),
      Symbol("Department") ->
        RecordType(Attributes(List(
          AttrType("dno", IntType()),
          AttrType("name", StringType()),
          AttrType("head", UserType(Symbol("Instructor"))),
          AttrType("instructors", UserType(Symbol("Instructors"))),
          AttrType("courses", UserType(Symbol("Courses")))))),
      Symbol("Departments") ->
        CollectionType(BagMonoid(), UserType(Symbol("Department"))),
      Symbol("Course") ->
        RecordType(Attributes(List(
          AttrType("code", StringType()),
          AttrType("name", StringType()),
          AttrType("offered_by", UserType(Symbol("Department"))),
          AttrType("taught_by", UserType(Symbol("Instructor"))),
          AttrType("is_prerequisite_for", UserType(Symbol("Courses"))),
          AttrType("has_prerequisites", CollectionType(SetMonoid(), UserType(Symbol("Course"))))))),
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
        RecordType(Attributes(List(
          AttrType("age", IntType())))))
    val manager =
      RecordType(Attributes(List(
        AttrType("name", StringType()),
        AttrType("children", children))))
    val employees =
      CollectionType(SetMonoid(),
        RecordType(Attributes(List(
          AttrType("dno", IntType()),
          AttrType("children", children),
          AttrType("manager", manager)))))
    val departments =
      CollectionType(SetMonoid(),
        RecordType(Attributes(List(AttrType("dno", IntType())))))

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
        RecordType(Attributes(List(
          AttrType("id", IntType()),
          AttrType("name", StringType())))))

    val courses =
      CollectionType(SetMonoid(),
        RecordType(Attributes(List(
          AttrType("cno", IntType()),
          AttrType("title", StringType())))))

    val transcripts =
      CollectionType(SetMonoid(),
        RecordType(Attributes(List(
          AttrType("id", IntType()),
          AttrType("cno", IntType())))))

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
        RecordType(Attributes(List(
          AttrType("RunNumber", IntType()),
          AttrType("lbn", IntType()),
          AttrType("muons",
            CollectionType(ListMonoid(),
              RecordType(Attributes(List(
                AttrType("pt", FloatType()),
                AttrType("eta", FloatType())))))),
          AttrType("jets",
            CollectionType(ListMonoid(),
              RecordType(Attributes(List(
                AttrType("pt", FloatType()),
                AttrType("eta", FloatType()))))))))))

    val goodRuns =
      CollectionType(ListMonoid(),
        RecordType(Attributes(List(
          AttrType("Run", IntType()),
          AttrType("OK", BoolType())))))

    val sources = Map(
      "Events" -> events,
      "GoodRuns" -> goodRuns
    )

    new World(sources)
  }

  def things = {
    val things =
      CollectionType(SetMonoid(),
        RecordType(Attributes(List(
          AttrType("a", IntType()),
          AttrType("b", IntType()),
          AttrType("set_a",
            CollectionType(SetMonoid(),
              FloatType())),
          AttrType("set_b",
            CollectionType(SetMonoid(),
              FloatType()))))))

    new World(sources = Map("things" -> things))
  }

  def fines = {
    val speed_limits =
      CollectionType(ListMonoid(),
        RecordType(Attributes(List(
          AttrType("location", StringType()),
          AttrType("min_speed", IntType()),
          AttrType("max_speed", IntType())))))

    val radar =
      CollectionType(ListMonoid(),
        RecordType(Attributes(List(
          AttrType("person", StringType()),
          AttrType("speed", IntType()),
          AttrType("location", StringType())))))

    val sources = Map(
      "speed_limits" -> speed_limits,
      "radar" -> radar
    )

    new World(sources)
  }

  def school = {
    val students =
      CollectionType(ListMonoid(),
        RecordType(Attributes(List(
          AttrType("name", StringType()),
          AttrType("birthYear", IntType()),
          AttrType("office", StringType()),
          AttrType("department", StringType())))))

    val profs =
      CollectionType(ListMonoid(),
        RecordType(Attributes(List(
          AttrType("name", StringType()),
          AttrType("office", StringType())))))

    val departments =
      CollectionType(ListMonoid(),
        RecordType(Attributes(List(
          AttrType("name", StringType()),
          AttrType("discipline", StringType()),
          AttrType("prof", StringType())))))

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
        RecordType(Attributes(List(
          AttrType("diagnostic_id", IntType()),
          AttrType("patient_id", IntType()),
          AttrType("diagnostic_code", StringType()),
          AttrType("diagnostic_type", StringType()),
          AttrType("diagnostic_date", StringType()),
          AttrType("hospital_id", StringType())))))

    val diagnosis_codes =
      CollectionType(ListMonoid(),
        RecordType(Attributes(List(
          AttrType("diagnostic_code", StringType()),
          AttrType("valid_for_coding", StringType()),
          AttrType("description", StringType())))))

    val sources = Map(
      "diagnosis" -> diagnosis,
      "diagnosis_codes" -> diagnosis_codes)

    new World(sources)
  }

  def set_of_tuples = {
    val tuples =
      CollectionType(SetMonoid(),
        RecordType(Attributes(List(
          AttrType("_1", IntType()),
          AttrType("_2", IntType()),
          AttrType("_3", IntType()),
          AttrType("_4", IntType())))))

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
      "records" -> CollectionType(SetMonoid(), RecordType(Attributes(List(AttrType("i", IntType()), AttrType("f", FloatType())))))))
  }

  def unknown = {
    new World(sources = Map(
      "unknownvalue" -> TypeVariable(),
      "unknown" -> CollectionType(SetMonoid(), TypeVariable()),
      "unknownrecords" -> CollectionType(SetMonoid(), RecordType(Attributes(List(AttrType("dead", TypeVariable()), AttrType("alive", TypeVariable())))))))
  }

  def professors_students = {
    val tipes = Map(
      Symbol("student") -> RecordType(Attributes(List(AttrType("name", StringType()), AttrType("age", IntType())))),
      Symbol("students") -> CollectionType(ListMonoid(), UserType(Symbol("student"))),
      Symbol("professor") -> RecordType(Attributes(List(AttrType("name", StringType()), AttrType("age", IntType())))),
      Symbol("professors") -> CollectionType(ListMonoid(), UserType(Symbol("professor"))))

    val sources = Map(
      "students" -> UserType(Symbol("students")),
      "professors" -> UserType(Symbol("professors")))

    new World(sources, tipes)
  }

  def empty = new World()

  def authors_publications = {
    val tipes = Map(
      Symbol("author") -> RecordType(Attributes(List(AttrType("name", StringType()), AttrType("title", StringType()), AttrType("year", IntType())))),
      Symbol("publication") -> RecordType(Attributes(List(AttrType("title", StringType()), AttrType("authors", CollectionType(BagMonoid(), StringType())), AttrType("affiliations", CollectionType(BagMonoid(), StringType())), AttrType("controlledterms", CollectionType(BagMonoid(), StringType()))))),
      Symbol("authors") -> CollectionType(BagMonoid(), UserType(Symbol("author"))),
      Symbol("publications") -> CollectionType(BagMonoid(), UserType(Symbol("publication"))))

    val sources = Map(
      "authors" -> UserType(Symbol("authors")),
      "publications" -> UserType(Symbol("publications")))

    new World(sources, tipes)
  }

  def text_file = {
    val sources = Map(
      "file" -> CollectionType(BagMonoid(), StringType())
    )

    new World(sources)
  }

  def nullables = {
    val tipes = Map(
      Symbol("nullable") -> RecordType(Attributes(List(AttrType("name", StringType()), AttrType("age", OptionType(IntType()))))),
      Symbol("nullables") -> CollectionType(BagMonoid(), UserType(Symbol("nullable")))
    )

    val sources = Map(
      "nullables" -> UserType(Symbol("nullables"))
    )

    new World(sources, tipes)
  }

}