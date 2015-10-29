package raw
package calculus

class ExpressionsDesugarerTest extends PhaseTest {

  val phase = "ExpressionsDesugarer"

  test("sum bag") {
    check(
      """sum(bag(1))""",
      """\$0 -> for ($1 <- $0) yield sum $1(bag(1))""")
  }

  test("sum list") {
    check(
      """sum(list(1))""",
      """\$0 -> for ($1 <- $0) yield sum $1(list(1))""")
  }

  test("max list") {
    check(
      """max(list(1))""",
      """\$0 -> for ($1 <- $0) yield max $1(list(1))""")
  }

  test("max set") {
    check(
      """max(set(1))""",
      """\$0 -> for ($1 <- $0) yield max $1(set(1))""")
  }

  test("count bag") {
    check(
      """count(bag(1))""",
      """\$0 -> for ($1 <- $0) yield sum 1(bag(1))""")
  }

  test("count list") {
    check(
      """count(list(1))""",
      """\$0 -> for ($1 <- $0) yield sum 1(list(1))""")
  }

  test("count set") {
    check(
      """count(set(1))""",
      """\$0 -> for ($1 <- $0) yield sum 1(to_bag(set(1)))""")
  }

  test("list(1,2,3)") {
    check(
      "list(1,2,3)",
      "list(1) append list(2) append list(3)")
  }

  test("x in L") {
    check(
      "0 in select A.year from A in authors",
      "for ($0 <- for (A$0 <- authors) yield $1 A$0.year) yield or $0 = 0",
      TestWorlds.publications)
  }

  test("select x in L from x in something") {
    // select all students having a birthyear matching any of the ones found for professors
    check(
      """select A.name from A in authors where A.title = "PhD" and A.year in select B.year from B in authors where B.title = "professor"""",
      """
       for (A$0 <- authors;
            A$0.title = "PhD"
            and for ($1 <- for (B$0 <- authors; B$0.title = "professor")
                            yield $3 B$0.year)
                yield or $1 = A$0.year
            )
       yield $4 A$0.name
      """,
      TestWorlds.publications)
  }

  test("a in publications.authors") {
    check(
      """select p from p in publications where "Donald Knuth" in p.authors""",
      """for (p$0 <- publications; for ($0 <- p$0.authors) yield or $0 = "Donald Knuth") yield $1 p$0""",
      TestWorlds.publications)
  }

}
