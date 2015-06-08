package raw.experiments

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import raw.repl.RawSparkContext

object PubsAndAuthorsSQL extends StrictLogging {

  val hiveContext: HiveContext = {
    val rawSparkContext = new RawSparkContext
    new HiveContext(rawSparkContext.sc)
  }

  val pubs: DataFrame = Common.newDataframeFromJSON(hiveContext, "data/pubs-authors/publications.json", "publications")
  val authors: DataFrame = Common.newDataframeFromJSON(hiveContext, "data/pubs-authors/authors.json", "authors")

  def main(args: Array[String]) {
    logger.info("Create rdd: " + pubs)
    logger.info("Create rdd: " + authors)

    // Articles which have more than one PhD student as an author.
    //    Common.runQuery( """
    //          SELECT * FROM (
    //              SELECT publications.title, COUNT(*) as phdAuthors
    //              FROM publications LATERAL VIEW explode(publications.authors) newTable AS authorsExp
    //              JOIN authors ON (authors.name = authorsExp AND authors.title = "PhD")
    //              GROUP BY publications.title
    //              ) AS t1
    //          WHERE phdAuthors > 1
    //                     	    """, repetitions = 0)

    //    Give me the articles whose authors are all the same age :) ?
    //    Common.runQuery( """
    //            SELECT * FROM (
    //              SELECT publications.title, COUNT(DISTINCT authors.year) distinctYears
    //              FROM publications LATERAL VIEW explode(publications.authors) newTable AS authorsExp
    //              JOIN authors ON (authors.name = authorsExp)
    //              GROUP BY publications.title) as t1
    //            WHERE distinctYears = 1
    //                     """, repetitions = 0)

    // Articles where one is a prof, one is a PhD student, but the prof is younger than the PhD student.
    /**
     * The two inner queries create two lists of (id, article, author name, author year) for both the phd authors and professor
     * authors of each article. There will be as many entries for each article as the number of phd (resp, professor)
     * authors in each article.
     *
     * To deal with articles with the same name but different author lists, it generates a column id = hash(title, authors)
     * before doing the LATERAL VIEW explode(). This way, if two articles have the same title, they can still be disinguished
     * in the outermost join by their id.
     */
        Common.runQuery("""
    SELECT phds.title, phds.name as phdName, phds.year as phdYear, profs.name as profName, profs.year as profYear
       FROM
          (SELECT t1.id, t1.title, authors.name, authors.year
            FROM (select hash(title, authors) id, * from publications) t1 LATERAL VIEW explode(t1.authors) newTable AS authorsExp
            JOIN authors ON (authors.name = authorsExp and (authors.title = "PhD"))) phds
       JOIN
          (SELECT t2.id, t2.title, authors.name, authors.year
            FROM (select hash(title, authors) id, * from publications) t2 LATERAL VIEW explode(t2.authors) newTable AS authorsExp
            JOIN authors ON (authors.name = authorsExp and (authors.title = "professor"))) profs
       ON (phds.id = profs.id)
       WHERE (phds.year < profs.year)
       ORDER BY phds.title
          """, repetitions = 0)

    Thread.sleep(1000000)
    /**
     * Same as above, except that instead of generating an id = hash(title, authors), it also preserves the authors of
     * the article (title, authors, phd or professor author, author year) and then joins on title and author.
     *
     * No difference in time from previous approach with 1000 publications.
     */
//    Common.runQuery( """
//SELECT phds.title, phds.name as phdName, phds.year as phdYear, profs.name as profName, profs.year as profYear
//   FROM
//      (SELECT publications.title, publications.authors, authors.name, authors.year
//        FROM publications LATERAL VIEW explode(authors) newTable AS authorsExp
//        JOIN authors ON (authors.name = authorsExp and (authors.title = "PhD"))) phds
//   JOIN
//      (SELECT publications.title, publications.authors, authors.name, authors.year
//        FROM publications LATERAL VIEW explode(authors) newTable AS authorsExp
//        JOIN authors ON (authors.name = authorsExp and (authors.title = "professor"))) profs
//   ON (phds.title = profs.title and phds.authors = profs.authors)
//   WHERE (phds.year < profs.year)
//                     """, repetitions = 10)


    //  Thread.sleep(1000000)

    //    runQuery( """SELECT authCount, collect_list(title) docs
    //	    FROM (SELECT title, size(authors) AS authCount FROM pubs) t2
    //	    GROUP BY authCount
    //	    ORDER BY authCount""")
    //
    //    runQuery( """SELECT authCount, count(title) articles
    //      FROM (SELECT title, size(authors) as authCount from pubs) t2
    //      GROUP BY authCount
    //      ORDER BY authCount""")
    //
    //    runQuery( """SELECT author, collect_list(title)
    //      FROM pubs LATERAL VIEW explode(authors) t2 AS author
    //      GROUP BY author
    //      ORDER BY author""")
    //
    //    runQuery( """SELECT author, count(title) AS titles
    //      FROM pubs LATERAL VIEW explode(authors) t2 AS author
    //      GROUP BY author
    //      ORDER BY author""")
    //
    //    runQuery( """SELECT t2.term, collect_list(title) AS titles
    //      FROM pubs LATERAL VIEW explode(controlledterms) t2 AS term
    //      GROUP BY t2.term""")
    //
    //    runQuery( """SELECT t2.term, count(title) AS titles
    //      FROM pubs LATERAL VIEW explode(controlledterms) t2 AS term
    //      GROUP BY t2.term """)
    //
    //    runQuery( """SELECT DISTINCT explode(authors) FROM pubs
    //	    WHERE array_contains(controlledterms, "particle detectors")""")
    //    	    (SELECT title, size(authors) AS authCount FROM publications) t2

    Thread.sleep(1000000)
  }
}
