package raw
package calculus

class OptimizerTest extends FunTest {

  def process(w: World, q: String) = {
    val ast = parse(q)

    val t = new Calculus.Calculus(ast)

    val analyzer = new calculus.SemanticAnalyzer(t, w)
    val t_q = analyzer.tipe(t.root)
    analyzer.errors.foreach(err => logger.error(ErrorsPrettyPrinter(err)))
    assert(analyzer.errors.length === 0)
    logger.debug(s"t_q $t_q")

    val algebra = Phases(t, w, lastTransform = Some("Optimizer"))

    val analyzer2 = new calculus.SemanticAnalyzer(algebra, w)
    analyzer2.errors.foreach(err => logger.error(ErrorsPrettyPrinter(err)))
    assert(analyzer2.errors.length === 0)
    val t_alg = analyzer2.tipe(algebra.root)

    compare(FriendlierPrettyPrinter(t_alg), FriendlierPrettyPrinter(t_q))

    algebra
  }

  def check(query: String, world: World, algebra: String) = compare(CalculusPrettyPrinter(process(world, query).root), algebra)

  test("exists") {
    check("exists(authors)", TestWorlds.publications,
     """reduce(or, $0 <- authors, true)""")
  }

  test("to_set found") {
    check("select A.title from A in (select distinct A from A in authors)", TestWorlds.publications,
      """reduce(set, $313 <- authors, $313.title)""")
  }

  test("publications: select A from A in authors") {
    check("select A from A in authors", TestWorlds.publications, """authors""")
  }

  test("publications: select A from A in authors where A.year = 1990") {
    check("select A from A in authors where A.year = 1990", TestWorlds.publications, """filter($0 <- authors, $0.year = 1990)""")
  }

  test("publications: should do an efficient group by #0") {
    check("select distinct partition from A in authors where A.year = 1992 group by A.title", TestWorlds.publications,
      """reduce(set, ($0, $92) <- m-nest($0 <- filter($0 <- authors, $0.year = 1992), (bag, $0.title, true, $0)), $92)""")
  }

  test("publications: should do an efficient group by #1") {
    check("select distinct count(partition) from A in authors where A.year = 1992 group by A.title", TestWorlds.publications,
      """reduce(set, ($0, $151) <- m-nest($0 <- filter($0 <- authors, $0.year = 1992), (sum, $0.title, true, 1)), $151)""")
  }

  test("publications: should do an efficient group by #2") {
    check("select distinct (select x.name from partition x) from A in authors where A.year = 1992 group by A.title", TestWorlds.publications,
      """reduce(
                  set,
                  ($0, $137) <- m-nest($0 <- filter($0 <- authors, $0.year = 1992), (bag, $0.title, true, $0.name)),
                  $137)""")
  }

  test("publications: should do an efficient group by #3") {
    // here the project of $38.title is rewritten into $38 since we nest on that now
    // TODO could get rid of the reduce! See test below
    check("select distinct A.title, (select x.year from partition x) from A in authors group by A.title", TestWorlds.publications,
      """reduce(set, ($0, $122) <- m-nest($0 <- authors, (bag, $0.title, true, $0.year)), (_1: $0.title, _2: $122))""")
  }

  test("publications: should do an efficient group by #3++") {
    // here the project of $38.title is rewritten into $38 since we nest on that now
    // and TODO reduce is removed (see test above)
    check("select distinct A.title, (select x.year from partition x) from A in authors group by A.title", TestWorlds.publications,
      """reduce(set, ($0, $1835) <- m-nest($0 <- authors, (bag, $0.title, true, $0.year)), (_1: $0.title, _2: $1835))""")
  }

  test("publications: should do an efficient group by") {
    check("select distinct A.title, select x.year from x in partition from A in authors group by A.title", TestWorlds.publications,
      """reduce(set, ($0, $122) <- m-nest($0 <- authors, (bag, $0.title, true, $0.year)), (_1: $0.title, _2: $122))""")
  }

  test("publications: count of authors grouped by title and then year") {
    // TODO our rule is not yet handling this case
    check("select distinct title: A.title, stats: (select year: A.year, N: max(select x.year from x in partition) from A in partition group by A.year) from A in authors group by A.title",
          TestWorlds.publications, """
          """)
  }

  test("publications: simple count of authors grouped by title and then year") {
    // TODO our rule is not yet handling this case
    check("select A.title, (select A.year, c:count(partition) from A in partition group by A.year) from A in authors group by A.title",
      TestWorlds.publications, """
          """)
  }

  test("a in publications.authors") {
    check("""select p from p in publications where "Donald Knuth" in p.authors""", TestWorlds.publications,
      """reduce(
    bag,
    ($38, $82) <- filter(
            ($38, $82) <- nest(
                    or,
                    ($38, $43) <- outer_unnest($38 <- publications, $43 <- $38.authors, true),
                    $38,
                    true,
                    $43 = "Donald Knuth"),
            $82),
    $38)""")
  }

  test("professors in publications") {
    check("""
    select P as publication,
    (select A
      from P.authors a, authors A
      where A.name = a and A.title = "professor") as profs
      from publications P 
    """, TestWorlds.authors_publications, 
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
    """)
  }

  test("nested nests") {
    // to see the shape of nest/outer-join chains, this leads to outer-join/nest/outer-join/nest/outer-join....
    check("select distinct A.title, sum(select a.year from a in partition), count(partition), max(select a.year from a in partition) from A in authors group by A.title",
      TestWorlds.publications, "")
  }

  test("nested nests 2") {
    // to see the shape of nest/outer-join chains, this leads to outer-join/outer-join/outer-join/..../nest/nest/nest/....
    check("select distinct A.title, (select distinct A.year, (select distinct A.name, count(partition) from partition A group by A.name) from A in partition group by A.year) from A in authors group by A.title",
      TestWorlds.publications, "")
  }


}