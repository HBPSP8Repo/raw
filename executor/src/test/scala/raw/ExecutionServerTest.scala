package raw

import raw.perf.QueryCompilerClient
import raw.publications.AbstractSparkPublicationsTest

class ExecutionServerTest extends AbstractSparkPublicationsTest with LDBDockerContainer {
  val countAuthors = """Reduce(SumMonoid(),
     IntConst(1),
     BoolConst(true),
     Select(BoolConst(true),
            Scan(authors,
                 BagType(RecordType(Seq(AttrType(title,StringType()),
                                        AttrType(name,StringType()),
                                        AttrType(year,IntType())),
                                    Author)))))
"""

  var queryCompiler: QueryCompilerClient = _

  override def beforeAll() {
    super.beforeAll()
    queryCompiler = new QueryCompilerClient(rawClassLoader)
  }

  def getResult(res: Either[String, RawQuery]) = res match {
    case Left(error) => fail(error)
    case Right(query) =>
      val result = query.computeResult
      println("Result: " + result)
      val resStr = convertToString(result)
      println("Result: " + resStr)
      resStr
  }

  //  test("countAuthors") {
  //    getResult(queryCompiler.compileLogicalPlan(countAuthors, accessPaths))
  //  }

  //  select distinct a.name as nom, a.title as titre, a.year as annee from authors a where a.year = 1973
  val authors1993 = """
  Reduce(SetMonoid(),
    RecordCons(Seq(AttrCons(annee,
      RecordProj(Arg(RecordType(Seq(AttrType(title,StringType()),
        AttrType(name,StringType()),
        AttrType(year,IntType())),
        Author)),
        year)),
      AttrCons(titre,
        RecordProj(Arg(RecordType(Seq(AttrType(title,StringType()),
          AttrType(name,StringType()),
          AttrType(year,IntType())),
          Author)),
          title)),
      AttrCons(nom,
        RecordProj(Arg(RecordType(Seq(AttrType(title,StringType()),
          AttrType(name,StringType()),
          AttrType(year,IntType())),
          Author)),
          name)))),
    BoolConst(true),
    Select(BinaryExp(Eq(),
      RecordProj(Arg(RecordType(Seq(AttrType(title,StringType()),
        AttrType(name,StringType()),
        AttrType(year,IntType())),
        Author)),
        year),
      IntConst(1973)),
      Scan(authors,
        SetType(RecordType(Seq(AttrType(title,StringType()),
          AttrType(name,StringType()),
          AttrType(year,IntType())),
          Author)))))
"""

  //  test("authors1993") {
  //    getResult(queryCompiler.compileLogicalPlan(authors1993, accessPaths))
  //  }

  // select author as a1, (select distinct title as t1, affiliations as aff from partition) as articles
  val qqq =
    """
    Reduce(BagMonoid(),
         RecordCons(Seq(AttrCons(articles,
                                 RecordProj(Arg(RecordType(Seq(AttrType(2,
                                                                        SetType(RecordType(Seq(AttrType(aff,
                                                                                                        ListType(StringType())),
                                                                                               AttrType(t1,
                                                                                                        StringType())),
                                                                                           None))),
                                                               AttrType(1,StringType())),
                                                           None)),
                                            2)),
                        AttrCons(a1,
                                 RecordProj(Arg(RecordType(Seq(AttrType(2,
                                                                        SetType(RecordType(Seq(AttrType(aff,
                                                                                                        ListType(StringType())),
                                                                                               AttrType(t1,
                                                                                                        StringType())),
                                                                                           None))),
                                                               AttrType(1,StringType())),
                                                           None)),
                                            1)))),
         BoolConst(true),
         Nest(SetMonoid(),
              RecordCons(Seq(AttrCons(aff,
                                      RecordProj(RecordProj(Arg(RecordType(Seq(AttrType(2,StringType()),
                                                                               AttrType(1,
                                                                                        RecordType(Seq(AttrType(title,
                                                                                                                StringType()),
                                                                                                       AttrType(controlledterms,
                                                                                                                ListType(StringType())),
                                                                                                       AttrType(authors,
                                                                                                                ListType(StringType())),
                                                                                                       AttrType(affiliations,
                                                                                                                ListType(StringType()))),
                                                                                                   Publication))),
                                                                           None)),
                                                            1),
                                                 affiliations)),
                             AttrCons(t1,
                                      RecordProj(RecordProj(Arg(RecordType(Seq(AttrType(2,StringType()),
                                                                               AttrType(1,
                                                                                        RecordType(Seq(AttrType(title,
                                                                                                                StringType()),
                                                                                                       AttrType(controlledterms,
                                                                                                                ListType(StringType())),
                                                                                                       AttrType(authors,
                                                                                                                ListType(StringType())),
                                                                                                       AttrType(affiliations,
                                                                                                                ListType(StringType()))),
                                                                                                   Publication))),
                                                                           None)),
                                                            1),
                                                 title)))),
              RecordProj(Arg(RecordType(Seq(AttrType(2,StringType()),
                                            AttrType(1,
                                                     RecordType(Seq(AttrType(title,StringType()),
                                                                    AttrType(controlledterms,
                                                                             ListType(StringType())),
                                                                    AttrType(authors,
                                                                             ListType(StringType())),
                                                                    AttrType(affiliations,
                                                                             ListType(StringType()))),
                                                                Publication))),
                                        None)),
                         2),
              BoolConst(true),
              IntConst(1),
              Unnest(RecordProj(Arg(RecordType(Seq(AttrType(title,StringType()),
                                                   AttrType(controlledterms,ListType(StringType())),
                                                   AttrType(authors,ListType(StringType())),
                                                   AttrType(affiliations,ListType(StringType()))),
                                               Publication)),
                                authors),
                     BinaryExp(Eq(),
                               RecordProj(Arg(RecordType(Seq(AttrType(2,StringType()),
                                                             AttrType(1,
                                                                      RecordType(Seq(AttrType(title,
                                                                                              StringType()),
                                                                                     AttrType(controlledterms,
                                                                                              ListType(StringType())),
                                                                                     AttrType(authors,
                                                                                              ListType(StringType())),
                                                                                     AttrType(affiliations,
                                                                                              ListType(StringType()))),
                                                                                 Publication))),
                                                         None)),
                                          2),
                               StringConst("Akoh, H.")),
                     Select(BoolConst(true),
                            Scan(publications,
                                 BagType(RecordType(Seq(AttrType(title,StringType()),
                                                        AttrType(controlledterms,
                                                                 ListType(StringType())),
                                                        AttrType(authors,ListType(StringType())),
                                                        AttrType(affiliations,ListType(StringType()))),
                                                    Publication)))))))
  """

  //  test("qqq") {
  //    getResult(queryCompiler.compileLogicalPlan(qqq, accessPaths))
  //  }

  test("compile oql") {
    getResult(queryCompiler.compileOQL("count(authors)", accessPaths))
  }


  //  test("compiled") {
  //    import raw.query._
  //    val res = (new CountAuthorPlanQuery(authorsRDD, publicationsRDD)).computeResult
  //    println(s"Result: $res")
  //  }

  //  test("LoadJar") {
  //    loadClass(Paths.get(System.getProperty("java.io.tmpdir"), "query.jar"), "raw.query.CountAuthorPlanQuery")
  //  }
  //
  //  def loadClass(jarFile:Path, queryClass:String): Unit = {
  //    var classLoader = new java.net.URLClassLoader(
  //      Array(jarFile.toUri.toURL),
  //      /*
  //       * need to specify parent, so we have all class instances
  //       * in current context
  //       */
  //      this.getClass.getClassLoader)
  //
  //    println("Classloader: " + classLoader)
  //
  ////    /*
  ////     * please note that the suffix "$" is for Scala "object",
  ////     * it's a trick
  ////     */
  ////    var clazzExModule = classLoader.loadClass(Module.ModuleClassName + "$")
  //
  //    var clazz = classLoader.loadClass(queryClass)
  //    println("Class: " + clazz)
  //    println("Constructors: " +clazz.getConstructors.mkString("\n"))
  //    println("Methods: " + clazz.getDeclaredMethods.mkString("\n"))
  //    val ctor=clazz.getConstructor(classOf[RDD[_]], classOf[RDD[_]])
  //    val rawQuery = ctor.newInstance(authorsRDD, publicationsRDD).asInstanceOf[RawQuery]
  //    val res = rawQuery.computeResult
  //    println("Result: " + res)
  //
  ////    val m = clazz.getDeclaredMethod("computeResult")
  ////    println("Method: " + m)
  ////    val result = m.invoke(rawQuery)
  ////    println("Result: " + result)
  //
  //
  ////    /*
  ////     * currently, I don't know how to check if clazzExModule is instance of
  ////     * Class[Module], because clazzExModule.isInstanceOf[Class[_]] always
  ////     * returns true,
  ////     * so I use try/catch
  ////     */
  ////    try {
  ////      //"MODULE$" is a trick, and I'm not sure about "get(null)"
  ////      var module = clazzExModule.getField("MODULE$").get(null).asInstanceOf[Module]
  ////    } catch {
  ////      case e: java.lang.ClassCastException =>
  ////        printf(" - %s is not Module\n", clazzExModule)
  ////    }
  //  }
}
