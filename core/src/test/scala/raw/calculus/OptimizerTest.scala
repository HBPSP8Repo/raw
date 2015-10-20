package raw
package calculus

class OptimizerTest extends CalculusTest {

  val phase = "Optimizer"

  test("exists") {
    check(
      "exists(authors)",
      """reduce(or, $0 <- authors, true)""",
      TestWorlds.publications)
  }

  test("to_set found") {
    check(
      "select A.title from A in (select distinct A from A in authors)",
      """reduce(set, A$0 <- authors, A$0.title)""",
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
      """filter($0 <- authors, $0.year = 1990)""",
      TestWorlds.publications)
  }

  test("publications: should do an efficient group by #0") {
    check(
      "select distinct partition from A in authors where A.year = 1992 group by A.title",
      """reduce(set, ($0, $92) <- m-nest($0 <- filter($0 <- authors, $0.year = 1992), (bag, $0.title, true, $0)), $92)""",
      TestWorlds.publications)
  }

  test("publications: should do an efficient group by #1") {
    check(
      "select distinct count(partition) from A in authors where A.year = 1992 group by A.title",
      """reduce(set, ($0, $151) <- m-nest($0 <- filter($0 <- authors, $0.year = 1992), (sum, $0.title, true, 1)), $151)""",
      TestWorlds.publications)
  }

  test("publications: should do an efficient group by #2") {
    check(
      "select distinct (select x.name from partition x) from A in authors where A.year = 1992 group by A.title",
      """reduce(
                  set,
                  ($0, $137) <- m-nest($0 <- filter($0 <- authors, $0.year = 1992), (bag, $0.title, true, $0.name)),
                  $137)""",
      TestWorlds.publications)
  }

  test("for (a <- authors) yield bag a") {
    check("for (a <- authors) yield bag a", "", TestWorlds.publications)
  }

  test("publications: should do an efficient group by #3") {
    // here the project of $38.title is rewritten into $38 since we nest on that now
    // TODO could get rid of the reduce! See test below
    check(
      "select distinct A.title, (select x.year from partition x) from A in authors group by A.title",
      """reduce(set, ($0, $122) <- m-nest($0 <- authors, (bag, $0.title, true, $0.year)), (_1: $0.title, _2: $122))""",
      TestWorlds.publications)
  }

  test("publications: should do an efficient group by #3++") {
    // here the project of $38.title is rewritten into $38 since we nest on that now
    // and TODO reduce is removed (see test above)
    check(
      "select distinct A.title, (select x.year from partition x) from A in authors group by A.title",
      """reduce(set, ($0, $1835) <- m-nest($0 <- authors, (bag, $0.title, true, $0.year)), (_1: $0.title, _2: $1835))""",
      TestWorlds.publications)
  }

  test("publications: should do an efficient group by") {
    check(
      "select distinct A.title, select x.year from x in partition from A in authors group by A.title",
      """reduce(set, ($0, $122) <- m-nest($0 <- authors, (bag, $0.title, true, $0.year)), (_1: $0.title, _2: $122))""",
      TestWorlds.publications)
  }

  test("publications: count of authors grouped by title and then year") {
    // TODO our rule is not yet handling this case
    check(
      "select distinct title: A.title, stats: (select year: A.year, N: max(select x.year from x in partition) from A in partition group by A.year) from A in authors group by A.title",
      """
      """,
      TestWorlds.publications)
  }

  test("publications: simple count of authors grouped by title and then year") {
    // TODO our rule is not yet handling this case
    check(
      "select A.title, (select A.year, c:count(partition) from A in partition group by A.year) from A in authors group by A.title",
      """
      """,
      TestWorlds.publications)
  }

  test("a in publications.authors") {
    check(
      """select p from p in publications where "Donald Knuth" in p.authors""",
      """
      reduce(
        bag,
        ($38, $82) <- filter(
                ($38, $82) <- nest(
                        or,
                        ($38, $43) <- outer_unnest($38 <- publications, $43 <- $38.authors, true),
                        $38,
                        true,
                        $43 = "Donald Knuth"),
                $82),
        $38)
      """,
      TestWorlds.publications)
  }

  test("professors in publications") {
    check(
      """
      select P as publication,
      (select A
        from P.authors a, authors A
        where A.name = a and A.title = "professor") as profs
        from publications P
      """,
      """
      reduce(
        bag,
        ($0, $3) <- nest(
          bag,
          (($0, $1), $2) <- outer_join(
            ($0, $1) <- outer_unnest($0 <- publications, $1 <- $0.authors, true),
            $2 <- filter($2 <- authors, $2.title = "professor"),
            $2.name = $1),
          $0,
          true,
          $2),
        (publication: $0, profs: $3))
      """,
      TestWorlds.authors_publications)
  }

  test("nested nests") {
    // to see the shape of nest/outer-join chains, this leads to outer-join/nest/outer-join/nest/outer-join....
    check(
      "select distinct A.title, sum(select a.year from a in partition), count(partition), max(select a.year from a in partition) from A in authors group by A.title",
      "",
      TestWorlds.publications)
  }

  test("nested nests 2") {
    // to see the shape of nest/outer-join chains, this leads to outer-join/outer-join/outer-join/..../nest/nest/nest/....
    check(
      "select distinct A.title, (select distinct A.year, (select distinct A.name, count(partition) from partition A group by A.name) from A in partition group by A.year) from A in authors group by A.title",
      "",
      TestWorlds.publications)
  }

  test("nested nests 2 without fields") {
    // to see the shape of nest/outer-join chains, this leads to outer-join/outer-join/outer-join/..../nest/nest/nest/....
    check(
      "select distinct A.title, (select distinct A.year, (select distinct A.name, count(partition) from partition A group by A.name) from A in partition group by A.year) from A in authors group by A.title",
      "",
      TestWorlds.publications)
  }

  test("polymorphic select") {
    // to see the shape of nest/outer-join chains, this leads to outer-join/outer-join/outer-join/..../nest/nest/nest/....
    check(
    """
      |{
      | group_by_age := \xs -> select x.age, partition from x in xs group by x.age;
      | (group_by_age(students), group_by_age(professors))
      |}
    """.stripMargin,
    "",
      TestWorlds.professors_students)
  }

  test("was failed query but looks ok now") {
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

  test("alternative to above") {
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

  test("alternative to above without field names") {
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

  test("slow query #1") {
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

  test("slow query #2") {
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

}