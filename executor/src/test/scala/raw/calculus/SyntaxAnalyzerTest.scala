package raw
package calculus

class SyntaxAnalyzerTest extends CalculusTest {

  def matches(q: String, expected: String): Unit = {
    matches(q, Some(expected))
  }

  /** Utility function to check whether parsed strings matches `expected` string.
    * If `expected` is empty, the parsed string must be the same as the input string `q`.
    */
  def matches(q: String, expected: Option[String] = None): Unit = {
    val ast = parse(q)
    val pretty = CalculusPrettyPrinter(ast, 200).replaceAll("\\s+", " ").trim()
    val e = expected.getOrElse(q).replaceAll("\\s+", " ").trim()
    logger.debug("\n\tInput: {}\n\tParsed: {}\n\tAST: {}", q, pretty, ast)

    assert(pretty === e)
  }

  /** Check whether two strings parse to the same AST.
    */
  def sameAST(q1: String, q2: String): Unit = {
    val ast1 = parse(q1)
    val pretty1 = CalculusPrettyPrinter(ast1, 200)
    val ast2 = parse(q2)
    val pretty2 = CalculusPrettyPrinter(ast2, 200)
    logger.debug("\nFirst String\n\tInput: {}\n\tParsed: {}\n\tAST: {}\nSecond String\n\tInput: {}\n\tParsed: {}\n\tAST: {}", q1, pretty1, ast1, q2, pretty2, ast2)

    assert(ast1 == ast2)
  }

  def parseError(q: String, expected: Option[String] = None): Unit = {
    val r = SyntaxAnalyzer(q)
    assert(r.isLeft)
    if (expected.isDefined)
      r match {
        case Left(err: SyntaxAnalyzer.NoSuccess) => assert(err.msg.contains(expected.get))
      }
  }

  def parseError(q: String, expected: String): Unit = parseError(q, Some(expected))

  test("true") {
    matches("true")
  }

  test("false") {
    matches("false")
  }

  test("1") {
    matches("1")
  }

  test("1.") {
    matches("1.")
  }

  test("1.1") {
    matches("1.1")
  }

  test("\"hello\"") {
    matches("\"hello\"")
  }

  test("true and true") {
    matches("true and true")
  }

  test("true or true and false") {
    sameAST("true or true and false", "true or (true and false)")
  }

  test("1 < 2") {
    matches("1 < 2")
  }

  test("1 < 2 < 3") {
    sameAST("1 < 2 < 3", "((1 < 2) < 3)")
  }

  test("1 + 2") {
    matches("1 + 2")
  }

  test("1 + 2 + 3") {
    matches("1 + 2 + 3")
  }

  test("1 in list(1)") {
    matches("1 in list(1)")
  }

  test("1 * 2") {
    matches("1 * 2")
  }

  test("1 + 2 * 3") {
    sameAST("1 + 2 * 3", "1 + (2 * 3)")
  }

  test("1 + 2 * 3 * 4") {
    sameAST("1 + 2 * 3 * 4", "1 + ((2 * 3) * 4)")
  }

  test("student.name") {
    matches("student.name")
  }

  test("student.address.street") {
    sameAST("student.address.street", "((student.address).street)")
  }

  test("student.name as name") {
    sameAST("student.name as name", "name: student.name")
  }

  test("student.name as name, student.age as age") {
    sameAST("student.name as name, student.age as age", "name: student.name, age: student.age")
  }

  test("student.address.street as street, student.address.number as number") {
    sameAST("student.address.street as street, student.address.number as number", "street: student.address.street, number: student.address.number")
  }

  test("1, 2") {
    sameAST("1, 2", "(_1: 1, _2: 2)")
  }

  test("1,2 #2") {
    sameAST("1, 2", "_1: 1, _2: 2")
  }

  test("1, 2, 3") {
    sameAST("1, 2, 3", "(_1: 1, _2: 2, _3: 3)")
  }

  test("1, (2, 3)") {
    sameAST("1, (2, 3)", "(_1: 1, _2: (_1: 2, _2: 3))")
  }

  test("(1, 2), 3") {
    sameAST("(1, 2), 3", "(_1: (_1: 1, _2: 2), _2: 3)")
  }

  test("for #1") {
    matches("for (s <- students) yield set s")
  }

  test("for #2") {
    sameAST("for (s <- students; s.age > 18) yield set s", "for (s <- students; ((s.age) > 18)) yield set s")
  }

  test("for #3") {
    matches("for (s <- students; c <- s.courses; c.rating > 10) yield set s")
  }

  test("for #4") {
    sameAST("for (s <- students; c <- s.info.courses; c.details.rating > 10) yield set s", "for (s <- students; c <- ((s.info).courses); (((c.details).rating) > 10)) yield set s")
  }

  test("for #5") {
    sameAST("for (s <- students) yield set name: s.name, s.age as student_age", "for (s <- students) yield set s.name as name, student_age: s.age")
  }

  test("for #6") {
    sameAST("for (s <- students) yield set name: s.name, (for (c <- s.courses) yield set c) as courses", "for (s <- students) yield set s.name as name, courses: (for (c <- s.courses) yield set c)")
  }

  test("backticks") {
    matches( """for (e <- Employees) yield set (`Employee Children`: e.children)""")
  }

  test("record projection #1") {
    matches( """("Foo", "Bar")._1""", """(_1: "Foo", _2: "Bar")._1""")
  }

  test("record projection #2") {
    matches(
      """((`Employee Name`: "Ben", Age: 35).`Employee Name`, "Foo")._1""",
      """(`Employee Name`: (`Employee Name`: "Ben", Age: 35).`Employee Name`, _2: "Foo")._1""")
  }

  test("expression block #1") {
    matches( """{ a := 1; a + 1 }""")
  }

  test("expression block #2") {
    matches( """for (d <- Departments) yield set { name := d.name; (deptName: name) }""")
  }

  test("expression block #3 - newline") {
    sameAST(
      """{
           a := 1;
           a + 1
         }""",
      """{
           a := 1
           a + 1
         }""")
  }

  test("patterns #1") {
    matches(
      """{ (a, b) := (1, 2); a + b }""",
      """{ (a, b) := (_1: 1, _2: 2); a + b }""")
  }

  test("patterns #2") {
    matches( """{ (a, (b, c)) := (_1: 1, _2: (_1: 2, _2: 3)); a + b + c }""")
  }

  test("patterns #3") {
    sameAST( """\(a, b) -> a + b + 1""", """\a, b -> a + b + 1""")
  }

  test("patterns #4") {
    matches(
      """for ((a, b) <- list((1, 2)) ) yield set a + b""",
      """for ((a, b) <- list((_1: 1, _2: 2))) yield set a + b""")
  }

  test("f(g(x))") {
    matches("f(g(x))")
  }

  test("f(g(x.foo))") {
    matches("f(g(x.foo))")
  }

  test("x.f(1)") {
    sameAST("x.f(1)", "(x.f)(1)")
  }

  test("x.f.g(1)") {
    sameAST("x.f.g(1)", "((x.f).g)(1)")
  }

  test("(x.f).g(1)") {
    sameAST("(x.f).g(1)", "((x.f).g)(1)")
  }

  test( """\x -> f(x)""") {
    matches( """\x -> f(x)""")
  }

  test( """\x -> x * 1""") {
    sameAST( """\x -> x * 1""", """\x -> (x * 1)""")
  }

  test( """\x -> 1 * x""") {
    sameAST( """\x -> 1 * x""", """\x -> (1 * x)""")
  }

  test( """\x -> f(g(x)) * 1""") {
    matches( """\x -> f(g(x)) * 1""")
  }

  test( """\(x,y) -> x + y""") {
    sameAST( """\(x,y) -> x + y""", """\x,y -> (x + y)""")
  }

  test("f(x, y)") {
    matches("f(x, y)")
  }

  test("parentheses - sum - none") {
    matches("a + b + c", "a + b + c")
  }

  test("parentheses - sum - redundant 1") {
    matches("(a + b) + c", "a + b + c")
  }

  test("parentheses - sum - redundant 2") {
    matches("((a + b) + c)", "a + b + c")
  }

  test("parentheses - sum - redundant 3") {
    matches("a + (b + c)", "a + b + c")
  }

  test("parentheses - sum - redundant 4") {
    matches("(a + (b + c))", "a + b + c")
  }

  test("parentheses - sum/mult - none") {
    matches("a + b * c", "a + b * c")
  }

  ignore("parentheses - sum/mult - required") {
    matches("(a + b) * c", "(a + b) * c")
  }

  ignore("parentheses - sum/mult - some required") {
    matches("((a + b) * c)", "(a + b) * c")
  }

  test("parentheses - sum/mult - redundant") {
    matches("a + (b * c)", "a + b * c")
  }

  test("parentheses - subtraction - none") {
    matches("a - b - c", "a - b - c")
  }

  test("parentheses - subtraction - redundant") {
    matches("(a - b) - c", "a - b - c")
  }

  ignore("parentheses - subtraction - required") {
    matches("a - (b - c)", "a - (b - c)")
  }

  test("parentheses - division - none") {
    matches("a / b / c", "a / b / c")
  }

  test("parentheses - division - redundant") {
    matches("(a / b) / c", "a / b / c")
  }

  ignore("parentheses - division - required") {
    matches("a / (b / c)", "a / (b / c)")
  }

  ignore("parentheses - division - some required") {
    matches("((a / b) / (c / d))", "a / b / (c / d)")
  }

  test("parentheses - unary minus - #51") {
    matches("-a", "- a")
  }

  test("parentheses - double unary minus - #51") {
    matches("-(-a)", "- - a")
  }

  test("parentheses - unary plus - #51") {
    matches("+a", "a")
  }

  test("parentheses - equality - redundant") {
    matches("(a = b)", "a = b")
  }

  test("parentheses - equality - none") {
    matches("a = b", "a = b")
  }

  ignore("parentheses - lt and comparison - constants") {
    matches("2 < 4 = false", "2 < 4 = false")
  }

  ignore("parentheses - lt and comparison - variables - none") {
    matches("a < b = c", "a < b = c")
  }

  test("parentheses - lt and comparison - variables - redundant") {
    matches("(a < b) = c", "a < b = c")
  }

  test("source code comments") {
    sameAST(
      """
        // compute the max of values
        for (v <- values) // yield set v
           yield max v
           // check if type inference assumes v is a int/float
      """,
      """for (v <- values) yield max v""")
  }

  test("cern events") {
    matches("for (e1 <- Events; e1.RunNumber > 100) yield set (muon: e1.muon)")
  }

  test("paper query 1") {
    matches( """for (el <- for (d <- Departments; d.name = "CSE") yield set d.instructors; e <- el; for (c <- e.teaches) yield or c.name = "cse5331") yield set (name: e.name, address: e.address)""")
  }

  test("paper query 2") {
    matches( """for (e <- Employees) yield set (E: e, M: for (c <- e.children; for (d <- e.manager.children) yield and c.age > d.age) yield sum 1)""")
  }

  test("#63 (support for !=)") {
    sameAST( """{ a := 1 != 2; a }""", """{ a := 1 <> 2; a }""")
  }

  test("#71 (keywords issue) #1") {
    matches( """{ v := nothing; v }""")
  }

  test("#71 (keywords issue) #2") {
    matches( """{ v := nottrue; v }""")
  }

  test("#71 (keywords issue) #3") {
    parseError( """for (v <- collection) yield unionv""", "illegal monoid")
  }

  test("#71 (keywords issue) #4") {
    parseError( """for (v <- collection) yield bag_unionv""", "illegal monoid")
  }

  test("#71 (keywords issue) #5") {
    parseError( """for (v <- collection) yield appendv""", "illegal monoid")
  }

  test("#71 (keywords issue) #6") {
    parseError( """for (v <- collection) yield setv""", "illegal monoid")
  }

  test("#71 (keywords issue) #7") {
    parseError( """for (v <- collection) yield listv""", "illegal monoid")
  }

  test("#71 (keywords issue) #8") {
    parseError( """for (v <- collection) yield bagv""", "illegal monoid")
  }

  test("#71 (keywords issue) #9") {
    parseError( """for (v <- booleans) yield andv""", "illegal monoid")
  }

  test("#71 (keywords issue) #10") {
    parseError( """for (v <- booleans) yield andfalse""", "illegal monoid")
  }

  test("#71 (keywords issue) #11") {
    parseError( """for (v <- booleans) yield orv""", "illegal monoid")
  }

  test("#71 (keywords issue) #12") {
    parseError( """for (v <- booleans) yield ortrue""", "illegal monoid")
  }

  test("#71 (keywords issue) #13") {
  }

  test("#71 (keywords issue) #14") {
  }

  test("#71 (keywords issue) #15") {
    parseError( """for (v <- numbers) yield maxv""", "illegal monoid")
  }

  test("#71 (keywords issue) #16") {
    parseError( """for (v <- numbers) yield max12""", "illegal monoid")
  }

  test("#71 (keywords issue) #17") {
    parseError( """for (v <- numbers) yield sumv""", "illegal monoid")
  }

  test("#71 (keywords issue) #18") {
    parseError( """for (v <- numbers) yield sum123""", "illegal monoid")
  }

  test("#71 (keywords issue) #19") {
    parseError( """for (v <- numbers) yield multiplyv""", "illegal monoid")
  }

  test("#71 (keywords issue) #20") {
    parseError( """for (v <- numbers) yield multiply123""", "illegal monoid")
  }

  test("#71 (keywords issue) #21") {
    matches( """for (r <- records; v := trueandfalse) yield set v""")
  }

  test("#71 (keywords issue) #22") {
    matches( """for (r <- records; v := falseortrue) yield set v""")
  }

  test("#71 (keywords issue) #23") {
    matches( """{ v := iftruethen1else2; v }""")
  }

  test("#92 (record projection priorities) #1") {
    sameAST( """for (t <- bools; ((t.A) = (t.B)) or true) yield set t""", """for (t <- bools; t.A = t.B or true) yield set t""")
  }

  test("#92 (record projection priorities) #2") {
    sameAST( """(t.A) and (t.B)""", """t.A and t.B""")
  }

  test("#52 (chained logical operators) #1") {
    sameAST( """(a < b) = c""", """a < b = c""")
  }

  test("#52 (chained logical operators) #2") {
    sameAST( """(2 < 4) = false""", """2 < 4 = false""")
  }

  test("select name from students") {
    matches("select name from students", "select name from <- students")
  }

  test("select name from s in students") {
    matches("select name from students", "select name from <- students")
  }

  test("select s.name from s in students") {
    matches("select s.name from s in students", "select s.name from s <- students")
  }

  test("select s.name as n from s in students") {
    sameAST("select s.name as n from s in students", "select n: s.name from s in students")
  }

  test("select s.name as n, age, s.size from s in students") {
    sameAST("select s.name as n, age, s.size from s in students",
      "select n: s.name, age: age, size: s.size from s in students")
  }

  test("select 2015 - birthyear as age from s in students") {
    sameAST("select 2015 - birthyear as age from s in students", "select age: (2015 - birthyear) from s in students")
  }

  test("select name from (select s.name from s in students)") {
    sameAST("select name from (select s.name from s in students)", "select name from select s.name from s in students")
  }

  test("select s.name from students s") {
    sameAST("select s.name from students s", "select s.name from s in students")
  }

  test("select s.name from students as s") {
    sameAST("select s.name from students as s", "select s.name from s in students")
  }

  test("select s.dept, count(partition) from students s group by s.dept") {
    sameAST("select s.dept, count(partition) from students s group by s.dept", "select dept: s.dept, _2: count(partition) from students s group by s.dept")
  }

  test("select dpt, count(partition) as n from students s group by dpt: s.dept") {
    sameAST("select dpt, count(partition) as n from students s group by dpt: s.dept", "select dpt: dpt, n: count(partition) from students s group by dpt: s.dept")
  }

  test("select - fun stuff #1") {
    sameAST(
      """select { dog_to_human_years := \x -> x*7; (name, dog_to_human_years(age)) } from dogs""",
      """select { dog_to_human_years := (\x -> (x*7)); (name: name, _2: (dog_to_human_years(age))) } from dogs""")
  }

  test("select - fun stuff #2") {
    sameAST(
      """{
           dog_to_human_years := \x -> x*7;
           select name, dog_to_human_years(age) as human_age from dogs
         }""",
      """{
           dog_to_human_years := (\x -> (x*7));
           select (name: name, human_age: (dog_to_human_years(age))) from dogs
         }""")
  }

  test("star") {
    matches(
      "select * from students",
      "select * from <- students")
  }

  test("list(1,2,3)") {
    matches("list(1, 2, 3)")
  }

  test("""set(("dbname", "authors"), ("dbname", "publications"))""") {
    sameAST("""set(("dbname", "authors"), ("dbname", "publications"))""",
            """set((_1: "dbname", _2: "authors"), (_1: "dbname", _2: "publications"))""")
  }

  // TODO: Test case using "as" (keyword) as a function argument fails with a bad/weird error

  ignore("""select s.age/10, (select x. from (s,p) in partition) from students s group by s.age/10""") {
    // TODO: must FAIL but w/ a proper error, not a crash!
    matches("""      select s.age/10, (select x. from (s,p) in partition) from students s group by s.age/10""")
  }

  test("recordCons") {
    matches("(age1: 15)")
  }

  test("(1,2) into (column1: _1, column2: _2)") {
    matches("(1,2) into (column1: _1, column2: _2)", "(_1: 1, _2: 2) into (column1: _1, column2: _2)")
  }

  test("(number: 1, 2) into (column1: _1, column2: _2)") {
    matches("(number: 1, 2) into (column1: _1, column2: _2)", "(number: 1, _2: 2) into (column1: _1, column2: _2)")
  }

  test("regexConst #1") {
    sameAST("""r"1"""", "r\"\"\"" + "1" + "\"\"\"")
  }

  test("regexConst #2") {
    sameAST("""r"(\\w+)"""", "r\"\"\"" + """(\w+)""" + "\"\"\"")
  }

  test("stringConst #1") {
    matches("\"\\\"\"")
  }

  test("stringConst #2") {
    matches("\"\\t\"")
  }

  test("invalid regex syntax") {
    parseError("""select row parse as r"(\w+)\s+" from file row""", "invalid regular expression")
  }

  test("""to_epoch("2015/01/02", "yyyy/MM/dd")""") {
    matches("""to_epoch("2015/01/02", "yyyy/MM/dd")""")
  }

  ignore("#7 operator prioriy on record projection bug") {
    matches(
      """
        |select name: p.name,
        |       surname: p.surname,
        |       birthdate: p.birthDate ,
        |       (select form: g.formType from p.patientBasedData.)
        |       from patients p
      """.stripMargin)
  }

  test("isNull #1") {
    matches("isNull(age, 18)")
  }

  test("x is null") {
    matches("x is null")
  }

  test("x is not null") {
    matches("x is not null")
  }

  test("not x is null") {
    sameAST("not x is null", "not (x is null)")
  }

}
