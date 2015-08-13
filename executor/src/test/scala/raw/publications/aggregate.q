min(select year from authors)

val expected = convertExpected("""
1951
""")

--

max(select year from authors)

val expected = convertExpected("""
1994
""")

--

sum(select year from authors)

val expected = convertExpected("""
98724
""")

--

avg(select year from authors)

val expected = convertExpected("""
1974
""")

--

count(select year from authors)

val expected = convertExpected("""
50
""")

--

unique(select year from authors)

val expected = convertExpected("""
false
""")

--

EXISTS (select year from authors)

val expected = convertExpected("""
true
""")