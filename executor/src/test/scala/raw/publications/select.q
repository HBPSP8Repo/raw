select distinct a.name, a.title, a.year from authors a where a.year = 1973

val expected = convertExpected("""
[name: Neuhauser, B., title: professor, year: 1973]
[name: Takeno, K., title: PhD, year: 1973]
""")

--

select distinct a.name as nom, a.title as titre, a.year as annee from authors a where a.year = 1973

val expected = convertExpected("""
[annee: 1973, nom: Neuhauser, B., titre: professor]
[annee: 1973, nom: Takeno, K., titre: PhD]
""")
