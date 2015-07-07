select distinct title, count(partition) as n from authors A group by title: A.title


val expected = convertExpected("""
[n: 11, title: assistant professor]
[n: 16, title: PhD]
[n: 18, title: professor]
[n: 5, title: engineer]
""")

--

select distinct title, (select distinct year from partition) as years from authors A group by title: A.title


val expected = convertExpected("""
[title: PhD, years: [1955, 1959, 1964, 1969, 1972, 1973, 1974, 1975, 1981, 1982, 1985, 1986, 1988, 1991, 1992]]
[title: assistant professor, years: [1951, 1952, 1959, 1960, 1977, 1981, 1983, 1989, 1994]]
[title: engineer, years: [1951, 1961, 1972, 1977, 1992]]
[title: professor, years: [1956, 1959, 1964, 1965, 1967, 1969, 1971, 1972, 1973, 1976, 1984, 1987, 1991, 1993, 1994]]
""")

--

select distinct year, (select distinct A from partition) as people from authors A group by title: A.year


val expected = convertExpected("""
[people: [[name: Akoh, H., title: professor, year: 1959], [name: James, R.D., title: assistant professor, year: 1959], [name: McVittie, J.P., title: assistant professor, year: 1959], [name: Takada, S., title: PhD, year: 1959]], year: 1959]
[people: [[name: Alba, G.P., title: assistant professor, year: 1960]], year: 1960]
[people: [[name: Anderson, C.C., title: PhD, year: 1992], [name: Matsumoto, Y., title: engineer, year: 1992]], year: 1992]
[people: [[name: Bellet-Amalric, E., title: PhD, year: 1964], [name: Kotsar, Y., title: professor, year: 1964], [name: Natarajan, B.R., title: professor, year: 1964], [name: Seneclauze, C.M., title: PhD, year: 1964]], year: 1964]
[people: [[name: Bing, D.D., title: PhD, year: 1955]], year: 1955]
[people: [[name: Bland, R.W., title: professor, year: 1984], [name: Tian, Ying, title: professor, year: 1984]], year: 1984]
[people: [[name: Cabrera, B., title: PhD, year: 1974]], year: 1974]
[people: [[name: Das, A., title: assistant professor, year: 1981], [name: Hu, Lili, title: PhD, year: 1981]], year: 1981]
[people: [[name: Dickson, S.C., title: professor, year: 1971]], year: 1971]
[people: [[name: Dignan, T.G., title: PhD, year: 1985]], year: 1985]
[people: [[name: Doisneau, B., title: professor, year: 1991], [name: Wuttig, M., title: PhD, year: 1991]], year: 1991]
[people: [[name: Ertan, H.B., title: assistant professor, year: 1952]], year: 1952]
[people: [[name: Gagnon, P., title: assistant professor, year: 1951], [name: Xu, Rongrong, title: engineer, year: 1951]], year: 1951]
[people: [[name: Gallion, P., title: engineer, year: 1961]], year: 1961]
[people: [[name: Ishibashi, K., title: engineer, year: 1972], [name: Stricker, D.A., title: PhD, year: 1972], [name: Tickle, R., title: professor, year: 1972]], year: 1972]
[people: [[name: Johnson, R.T., title: professor, year: 1994], [name: Martoff, C.J., title: assistant professor, year: 1994], [name: Nakagawa, H., title: assistant professor, year: 1994]], year: 1994]
[people: [[name: Katase, A., title: PhD, year: 1988]], year: 1988]
[people: [[name: Khurgin, J., title: PhD, year: 1986]], year: 1986]
[people: [[name: Kokorin, V.V., title: professor, year: 1965]], year: 1965]
[people: [[name: Lee, A., title: engineer, year: 1977], [name: Zhang, Junjie, title: assistant professor, year: 1977]], year: 1977]
[people: [[name: Monroy, E., title: PhD, year: 1969], [name: Vey, J.-L., title: professor, year: 1969]], year: 1969]
[people: [[name: Murdock, E.S., title: assistant professor, year: 1989]], year: 1989]
[people: [[name: Neuhauser, B., title: professor, year: 1973], [name: Takeno, K., title: PhD, year: 1973]], year: 1973]
[people: [[name: Oae, Y., title: professor, year: 1967]], year: 1967]
[people: [[name: Sakae, T., title: assistant professor, year: 1983]], year: 1983]
[people: [[name: Sarigiannidou, E., title: PhD, year: 1975]], year: 1975]
[people: [[name: Shield, T., title: PhD, year: 1982]], year: 1982]
[people: [[name: Sun, Guoliang, title: professor, year: 1987]], year: 1987]
[people: [[name: Tozoni, O.V., title: professor, year: 1976]], year: 1976]
[people: [[name: Wang, Hairong, title: professor, year: 1993], [name: Zhuangde Jiang, title: professor, year: 1993]], year: 1993]
[people: [[name: Young, B.A., title: professor, year: 1956]], year: 1956]
""")