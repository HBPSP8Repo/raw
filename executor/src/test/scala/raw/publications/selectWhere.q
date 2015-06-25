select a from authors a where a.title = "PhD"

val authors: ImmutableMultiset[Author] = result.asInstanceOf[ImmutableMultiset[Author]]
val scalaIter = JavaConversions.asScalaIterator(authors.iterator())
val actual = scalaIter
  .map(a => s"${a.name}; ${a.title}; ${a.year}")
  .toList
  .sorted
  .mkString("\n")
val expected = """Anderson, C.C.; PhD; 1992
Bellet-Amalric, E.; PhD; 1964
Bing, D.D.; PhD; 1955
Cabrera, B.; PhD; 1974
Dignan, T.G.; PhD; 1985
Hu, Lili; PhD; 1981
Katase, A.; PhD; 1988
Khurgin, J.; PhD; 1986
Monroy, E.; PhD; 1969
Sarigiannidou, E.; PhD; 1975
Seneclauze, C.M.; PhD; 1964
Shield, T.; PhD; 1982
Stricker, D.A.; PhD; 1972
Takada, S.; PhD; 1959
Takeno, K.; PhD; 1973
Wuttig, M.; PhD; 1991""".replaceAll("""\n\s*""", "\n")
assert(actual === expected, s"Actual: $actual\nExpected: $expected")

--

select P from publications P
where "particle detectors" in P.controlledterms
    and "Hewlett-Packard Lab., Palo Alto, CA, USA" in P.affiliations
    and "Sarigiannidou, E." in P.authors


val pubs: ImmutableMultiset[Publication] = result.asInstanceOf[ImmutableMultiset[Publication]]
val scalaIter = JavaConversions.asScalaIterator(pubs.iterator())
val actual = scalaIter
  .map(p => s"${p.title}; ${p.authors}; ${p.affiliations}; ${p.controlledterms}")
  .toList
  .sorted
  .mkString("\n")
val expected = """Energies for atomic emissions from defect sites on the Si surfaces: The effects of halogen adsorbates; Sarigiannidou, E.; Hewlett-Packard Lab., Palo Alto, CA, USA, Dept. of Phys., Stanford Univ., CA, USA
Growth of epitaxial YbBa<inf>2</inf>Cu<inf>3</inf>O<inf>7</inf> superconductor by liquid&#x2010;gas&#x2010;solidification processing; Ertan, H.B., Cabrera, B., Zhuangde Jiang, Monroy, E., McVittie, J.P., Sarigiannidou, E.; Hewlett-Packard Lab., Palo Alto, CA, USA, Dept. of Aerosp. Eng. & Mech., Minnesota Univ., Minneapolis, MN, USA
Improvement of the stability of high-voltage generators for perturbations within a frequency bandwidth of 0.03--1000 Hz; Monroy, E., Kokorin, V.V., Stricker, D.A., Tickle, R., Dickson, S.C., Sarigiannidou, E.; Hewlett-Packard Lab., Palo Alto, CA, USA
MOCVD <formula formulatype=inline> <img src=/images/tex/19426.gif alt=\\hbox {Ge}_{3}\\hbox {Sb}_{2}\\hbox {Te}_{5}> </formula> for PCM Applications; Lee, A., Sarigiannidou, E., Tozoni, O.V.; Hewlett-Packard Lab., Palo Alto, CA, USA, Dept. of Phys. & Astron., San Francisco State Univ., CA, USA
Microstructure and phase composition evolution of nano-crystalline carbon films: Dependence on deposition temperature; Bing, D.D., Neuhauser, B., Sarigiannidou, E.; Hewlett-Packard Lab., Palo Alto, CA, USA
Optimization of the Scheduler for the Non-Blocking High-Capacity Router; Sarigiannidou, E., Neuhauser, B., Young, B.A.; Hewlett-Packard Lab., Palo Alto, CA, USA
[Front cover]; McVittie, J.P., Sarigiannidou, E.; Hewlett-Packard Lab., Palo Alto, CA, USA, CEA-Grenoble, INAC/SP2M/NPSC, 17 Rue des Martyrs, 38054 Grenoble cedex 9, France""".replaceAll("""\n\s*""", "\n")
assert(actual === expected, s"Actual: $actual\nExpected: $expected")

--
select P from publications P
where "particle detectors" in P.controlledterms
and "elemental semiconductors" in P.controlledterms
and "magnetic levitation" in P.controlledterms
and "titanium" in P.controlledterms
and "torque" in P.controlledterms

val pubs: ImmutableMultiset[Publication] = result.asInstanceOf[ImmutableMultiset[Publication]]
val scalaIter = JavaConversions.asScalaIterator(pubs.iterator())
val actual = scalaIter
  .map(p => s"${p.title}; ${p.authors}; ${p.affiliations}; ${p.controlledterms}")
  .toList
  .sorted
  .mkString("\n")
val expected = """Electron field emission from polycrystalline silicon tips; Gallion, P.; Dept. of Nucl. Eng., Kyushu Univ., Fukuoka, Japan
Heating of a dense plasma using a relativistic electron beam; Doisneau, B., Tickle, R., Das, A., Bland, R.W.; Dept. of Nucl. Eng., Kyushu Univ., Fukuoka, Japan
On Communication Over Unknown Sparse Frequency-Selective Block-Fading Channels; Johnson, R.T., Dickson, S.C., Lee, A., Matsumoto, Y., Xu, Rongrong, Cabrera, B.; Key Laboratory of Materials for High Power Laser, Shanghai Institute of Optics and Fine Mechanics, Chinese Academy of Sciences, Shanghai 201800, People&#x2019, Dept. of Electr. & Electron. Eng., Middle East Tech. Univ., Ankara, Turkey""".replaceAll("""\n\s*""", "\n")
assert(actual === expected, s"Actual: $actual\nExpected: $expected")