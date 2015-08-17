package raw.publications.generated

import raw.publications.AbstractSparkPublicationsTest
import raw.datasets.publications.Publications

class SelectWhereTest extends AbstractSparkPublicationsTest(Publications.publications) {

  test("SelectWhere0") {
    val oql = """
          select a from authors a where a.title = "PhD"
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    val actual = convertToString(result)

    val expected = convertExpected("""
    [name: Anderson, C.C., title: PhD, year: 1992]
    [name: Bellet-Amalric, E., title: PhD, year: 1964]
    [name: Bing, D.D., title: PhD, year: 1955]
    [name: Cabrera, B., title: PhD, year: 1974]
    [name: Dignan, T.G., title: PhD, year: 1985]
    [name: Hu, Lili, title: PhD, year: 1981]
    [name: Katase, A., title: PhD, year: 1988]
    [name: Khurgin, J., title: PhD, year: 1986]
    [name: Monroy, E., title: PhD, year: 1969]
    [name: Sarigiannidou, E., title: PhD, year: 1975]
    [name: Seneclauze, C.M., title: PhD, year: 1964]
    [name: Shield, T., title: PhD, year: 1982]
    [name: Stricker, D.A., title: PhD, year: 1972]
    [name: Takada, S., title: PhD, year: 1959]
    [name: Takeno, K., title: PhD, year: 1973]
    [name: Wuttig, M., title: PhD, year: 1991]
    """)

    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
  }

  test("SelectWhere1") {
    val oql = """
          select a from authors a
    where a.year = 1973 or a.year = 1975
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    val actual = convertToString(result)

    val expected = convertExpected("""
    [name: Neuhauser, B., title: professor, year: 1973]
    [name: Sarigiannidou, E., title: PhD, year: 1975]
    [name: Takeno, K., title: PhD, year: 1973]
    """)

    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
  }

  test("SelectWhere2") {
    val oql = """
          select P from publications P
    where "particle detectors" in P.controlledterms
        and "Hewlett-Packard Lab., Palo Alto, CA, USA" in P.affiliations
        and "Sarigiannidou, E." in P.authors
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    val actual = convertToString(result)

    val expected = convertExpected("""
    
    [affiliations: [CEA-Grenoble, INAC/SP2M/NPSC, 17 Rue des Martyrs, 38054 Grenoble cedex 9, France, Hewlett-Packard Lab., Palo Alto, CA, USA], authors: [McVittie, J.P., Sarigiannidou, E.], controlledterms: [neutrino detection and measurement, particle detectors, scanning electron microscope examination of materials, superconducting junction devices, superconductive tunnelling, titanium, torque], title: [Front cover]]
    [affiliations: [Dept. of Aerosp. Eng. & Mech., Minnesota Univ., Minneapolis, MN, USA, Hewlett-Packard Lab., Palo Alto, CA, USA], authors: [Cabrera, B., Ertan, H.B., McVittie, J.P., Monroy, E., Sarigiannidou, E., Zhuangde Jiang], controlledterms: [X-ray detection and measurement, grain size, particle detectors, reluctance motors, superconducting junction devices, superconducting thin films], title: Growth of epitaxial YbBa<inf>2</inf>Cu<inf>3</inf>O<inf>7</inf> superconductor by liquid&#x2010;gas&#x2010;solidification processing]
    [affiliations: [Dept. of Phys. & Astron., San Francisco State Univ., CA, USA, Hewlett-Packard Lab., Palo Alto, CA, USA], authors: [Lee, A., Sarigiannidou, E., Tozoni, O.V.], controlledterms: [elemental semiconductors, lattice phonons, magnetic levitation, particle detectors, reluctance motors, superconducting thin films, torque], title: MOCVD <formula formulatype="inline"> <img src="/images/tex/19426.gif" alt="\hbox {Ge}_{3}\hbox {Sb}_{2}\hbox {Te}_{5}"> </formula> for PCM Applications]
    [affiliations: [Dept. of Phys., Stanford Univ., CA, USA, Hewlett-Packard Lab., Palo Alto, CA, USA], authors: [Sarigiannidou, E.], controlledterms: [lattice phonons, neutrino detection and measurement, particle detectors, stepping motors], title: Energies for atomic emissions from defect sites on the Si surfaces: The effects of halogen adsorbates]
    [affiliations: [Hewlett-Packard Lab., Palo Alto, CA, USA], authors: [Bing, D.D., Neuhauser, B., Sarigiannidou, E.], controlledterms: [elemental semiconductors, neutrino detection and measurement, particle detectors, reluctance motors, superconducting junction devices, superconducting thin films, superconductive tunnelling, torque], title: Microstructure and phase composition evolution of nano-crystalline carbon films: Dependence on deposition temperature]
    [affiliations: [Hewlett-Packard Lab., Palo Alto, CA, USA], authors: [Dickson, S.C., Kokorin, V.V., Monroy, E., Sarigiannidou, E., Stricker, D.A., Tickle, R.], controlledterms: [X-ray detection and measurement, magnetic levitation, particle detectors, scanning electron microscope examination of materials, stepping motors, superconducting junction devices, superconductive tunnelling, torque], title: Improvement of the stability of high-voltage generators for perturbations within a frequency bandwidth of 0.03--1000 Hz]
    [affiliations: [Hewlett-Packard Lab., Palo Alto, CA, USA], authors: [Neuhauser, B., Sarigiannidou, E., Young, B.A.], controlledterms: [neutrino detection and measurement, particle detectors, reluctance motors, scanning electron microscope examination of materials, silicon, superconducting junction devices, superconductive tunnelling, torque], title: Optimization of the Scheduler for the Non-Blocking High-Capacity Router]
    """)

    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
  }

  test("SelectWhere3") {
    val oql = """
          select P from publications P
    where "particle detectors" in P.controlledterms
    and "elemental semiconductors" in P.controlledterms
    and "magnetic levitation" in P.controlledterms
    and "titanium" in P.controlledterms
    and "torque" in P.controlledterms
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    val actual = convertToString(result)

    val expected = convertExpected("""
    
    [affiliations: [Dept. of Electr. & Electron. Eng., Middle East Tech. Univ., Ankara, Turkey, Key Laboratory of Materials for High Power Laser, Shanghai Institute of Optics and Fine Mechanics, Chinese Academy of Sciences, Shanghai 201800, People&#x2019], authors: [Cabrera, B., Dickson, S.C., Johnson, R.T., Lee, A., Matsumoto, Y., Xu, Rongrong], controlledterms: [elemental semiconductors, magnetic flux, magnetic levitation, particle detectors, silicon, superconducting junction devices, titanium, torque], title: On Communication Over Unknown Sparse Frequency-Selective Block-Fading Channels]
    [affiliations: [Dept. of Nucl. Eng., Kyushu Univ., Fukuoka, Japan], authors: [Bland, R.W., Das, A., Doisneau, B., Tickle, R.], controlledterms: [elemental semiconductors, magnetic levitation, neutrino detection and measurement, particle detectors, superconducting junction devices, superconducting thin films, titanium, torque], title: Heating of a dense plasma using a relativistic electron beam]
    [affiliations: [Dept. of Nucl. Eng., Kyushu Univ., Fukuoka, Japan], authors: [Gallion, P.], controlledterms: [elemental semiconductors, magnetic levitation, particle detectors, superconducting thin films, titanium, torque], title: Electron field emission from polycrystalline silicon tips]
    """)

    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
  }

}
