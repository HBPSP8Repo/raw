select distinct a.name, a.title, a.year from authors a where a.year = 1973

val resultsAsList:Set[List[String]] = result
      .map(a => List(s"name: ${a.name}", s"title: ${a.title}", s"year: ${a.year}"))
      .asInstanceOf[Set[List[String]]]
val actual = resultsToString(resultsAsList)
val expected = convertExpected("""
[name: Neuhauser, B., title: professor, year: 1973]
[name: Takeno, K., title: PhD, year: 1973]
""")

--

select distinct a.name as nom, a.title as titre, a.year as annee from authors a where a.year = 1973

val resultsAsList:Set[List[String]] = result
  .map(a => List(s"nom: ${a.nom}", s"titre: ${a.titre}", s"annee: ${a.annee}"))
  .asInstanceOf[Set[List[String]]]
val actual =  resultsToString(resultsAsList)
val expected = convertExpected("""
[annee: 1973, nom: Neuhauser, B., titre: professor]
[annee: 1973, nom: Takeno, K., titre: PhD]
""")
