package raw
package calculus

import org.kiama.rewriting.Cloner._
import Calculus._

class OptimizerTest extends PhaseTest {

  val phase = "Optimizer"

  def check_nest2(q: String, w: World, n:Int = 1): Unit = {
    val tree = new Calculus.Calculus(parse(q))
    val analyzer = new SemanticAnalyzer(tree, w, q)
    assert(analyzer.errors.isEmpty)

    val tree2 = Phases(tree, w, q, lastTransform = Some(phase))
    val analyzer1 = new SemanticAnalyzer(tree2, w, "")
    assert(analyzer1.errors.isEmpty)
    val collect_nests = collect[Seq, Nest] {
      case x: Nest => x
    }
    val collect_nests2 = collect[Seq, Nest2] {
      case x: Nest2 => x
    }

    val nests = collect_nests(tree2.root)
    val nests2 = collect_nests2(tree2.root)

    logger.debug(s"${CalculusPrettyPrinter(tree.root)}\nbecame\n${CalculusPrettyPrinter(tree2.root)}")

    assert(nests.length == 0)
    assert(nests2.length == n)


  }

  test("exists") {
    check(
      "exists(authors)",
      """reduce(or, $0 <- authors, true)""",
      TestWorlds.publications)
  }

  test("publications: select A from A in authors") {
    check(
      "select A from A in authors",
      """authors""",
      TestWorlds.publications,
      ignoreRootTypeComparison = true)
  }

  test("publications: select A from A in authors where A.year = 1990") {
    check(
      "select A from A in authors where A.year = 1990",
      """filter(A$0 <- authors, A$0.year = 1990)""",
      TestWorlds.publications, ignoreRootTypeComparison = true)
  }

  test("publications: should do an efficient group by #0") {
    check_nest2("select distinct partition from A in authors where A.year = 1992 group by A.title", TestWorlds.publications)
  }

  test("publications: should do an efficient group by #1") {
    check_nest2("select distinct count(partition) from A in authors where A.year = 1992 group by A.title", TestWorlds.publications)
  }

  test("publications: should do an efficient group by #2") {
    check_nest2("select distinct (select x.name from partition x) from A in authors where A.year = 1992 group by A.title", TestWorlds.publications)
  }

  test("for (a <- authors) yield bag a") {
    check("for (a <- authors) yield bag a", "authors", TestWorlds.publications)
  }

  test("publications: should do an efficient group by #3") {
    check_nest2("select distinct A.title, (select x.year from partition x) from A in authors group by A.title", TestWorlds.publications)
  }

  test("publications: should do an efficient group by #3++") {
    check_nest2("select distinct A.title, (select x.year from partition x) from A in authors group by A.title", TestWorlds.publications)
  }

  test("publications: should do an efficient group by") {
    check_nest2("select distinct A.title, (select x.year from x in partition) from A in authors group by A.title", TestWorlds.publications)
  }

  test("publications: count of authors grouped by title and then year") {
    // TODO our rule is not yet handling this case
    check_nest2(
      "select distinct title: A.title, stats: (select year: A.year, N: max(select x.year from x in partition) from A in partition group by A.year) from A in authors group by A.title",
      TestWorlds.publications, 2)
  }

  test("publications: simple count of authors grouped by title and then year") {
    // TODO our rule is not yet handling this case
    check_nest2(
      "select A.title, (select A.year, c:count(partition) from A in partition group by A.year) from A in authors group by A.title",
      TestWorlds.publications, 2)
  }

  test("nested nests") {
    // to see the shape of nest/outer-join chains, this leads to outer-join/nest/outer-join/nest/outer-join....
    check_nest2(
      "select distinct A.title, sum(select a.year from a in partition), count(partition), max(select a.year from a in partition) from A in authors group by A.title",
      TestWorlds.publications, 3)
  }

  test("nested nests 2") {
    // to see the shape of nest/outer-join chains, this leads to outer-join/outer-join/outer-join/..../nest/nest/nest/....
    check_nest2(
      "select distinct A.title, (select distinct A.year, (select distinct A.name, count(partition) from partition A group by A.name) from A in partition group by A.year) from A in authors group by A.title",
      TestWorlds.publications, 3)
  }

  ignore("(probably to delete) was failed query but looks ok now") {
    check(
      """
      select G.title as title,
             (select year: v,
                     N: count(partition)
              from v in G.values
              group by v) as stats
      from (
        select distinct A.title as title,
                        (select A.year from A in partition) as values
        from authors A
        group by A.title) G
      """,
      """
      """,
      TestWorlds.publications)
  }

  ignore("(probably to delete) alternative to above") {
    check(
      """
      select distinct A.title,
             sum(select a.year from a in partition),
             count(partition),
             max(select a.year from a in partition)
      from A in authors
      group by A.title
      """,
      """""",
      TestWorlds.publications)
  }

  ignore("(probably to delete) alternative to above without field names") {
    check(
      """
      select distinct title,
             sum(select year from partition),
             count(partition),
             max(select year from partition)
      from authors
      group by title
      """,
      """""",
      TestWorlds.publications)
  }

  ignore("(probably to delete) slow query #1") {
    check(
      """
      select G.n,
             (select a.article from a in partition) as articles
      from (
        select P as article,
               count(P.authors) as n
        from publications P) G
      group by G.n
      """,
      """""",
      TestWorlds.publications)
  }

  ignore("(probably to delete) slow query #2") {
    check(
      """
      select A as author,
             partition as articles
      from publications P, A in P.authors
      group by A
      """,
      """""",
      TestWorlds.publications)
  }

  ignore("(probably to delete) polymorphic select w/ group by") {
    check(
      """
      {
        group_by_year := \table -> select t.year, * from table t group by t.year;
        group_by_year(authors)
      }
      """,
      """
      reduce(
        bag,
        (t$0, $2) <- nest(
          bag,
          (t$0, t$1) <- outer_join(t$0 <- authors, t$1 <- authors, t$0.year = t$1.year),
          t$0,
          true,
          t$1),
        (year: t$0.year, _2: $2))
      """,
      TestWorlds.publications,
      ignoreRootTypeComparison = true)
  }

}