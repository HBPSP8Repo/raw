import raw._
import raw.executor.reference.ReferenceExecutor


object ExamplesPhysicalPlan {
  def main (args: Array[String]) {

    val movies = Source(
      CollectionType(ListMonoid(), RecordType(List(AttrType("title", StringType()), AttrType("year", IntType()), AttrType("actors", CollectionType(SetMonoid(), StringType()))))),
      LocalFileLocation("src/test/data/smokeTest/movies.json", "application/json")
    )

    val actors = Source(
      CollectionType(ListMonoid(), RecordType(List(AttrType("name", StringType()), AttrType("born", IntType())))),
      LocalFileLocation("src/test/data/smokeTest/actors.json", "application/json")
    )

    val nestedRecordType = RecordType(List(AttrType("kidsNo",IntType())))
    val actorsSpouses = Source(
      CollectionType(ListMonoid(), RecordType(List(AttrType("name", StringType()), AttrType("spouse", StringType()),
        AttrType("various", nestedRecordType)))),
      LocalFileLocation("src/test/data/smokeTest/actorsSpouses.json", "application/json")
    )

    val profs = Source(
      CollectionType(ListMonoid(),RecordType(List(AttrType("name",StringType()), AttrType("office", StringType())))),
      LocalFileLocation("src/test/data/smokeTest/profs.csv", "text/csv")
    )

    val world = new World(Map(
      "movies" -> movies,
      "actors" -> actors,
      "actorsSpouses" -> actorsSpouses,
      "profs"  -> profs
    ))

    val queryStr = "for (m <- movies, m.title = \"Seven\", a <- m.actors) yield set a"

    val queryStr2 = "for (m <- movies, a <- actors, ma <- m.actors, a.name = ma, a.born > 1960) yield set m.title"

    /* Breaks Executor */
    val queryStr3 = "for (a1 <- actors, a2 <- actorsSpouses, a1.name = a2.name, m <- movies, ma <- m.actors, a2.name " +
      "= ma) yield set 2 + m.year"

    val queryStr4 = "for (m <- movies, a2 <- actorsSpouses, a1 <- actors, a1.name = a2.name, ma <- m.actors, a2.name " +
      "= ma) yield set m.title"

    val queryStr5 = "for (m <- movies, a2 <- actorsSpouses, a1 <- actors, a1.name = a2.name, ma <- m.actors, a2.name " +
      "= ma) yield set m"

    val queryStr6 = "for (m <- movies, a2 <- actorsSpouses, a1 <- actors, a1.name = a2.name, ma <- m.actors, a2.name " +
      "= ma) yield set a2.various.kidsNo"

    /* Very neat simplification */
    val queryStr7 = "for (m <- movies, a2 <- actorsSpouses, a1 <- actors, a1.name = a2.name, ma <- m.actors, a2.name " +
      "= ma) yield set (x := a2.various.kidsNo, y:= a2.name).x"

    val queryStr8 = "for (m <- movies, a2 <- actorsSpouses, a1 <- actors, a1.name = a2.name, ma <- m.actors, a2.name " +
      "= ma) yield set (x := a2.various.kidsNo, y:= a2.name)"

    val executor = ReferenceExecutor

    /* Queries 4 and 8 unnecessarily introduce Merge Monoid */
    val result = PhysicalPlan(queryStr4, world, executor)

  }
}
