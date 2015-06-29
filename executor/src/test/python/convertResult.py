from collections import OrderedDict

def listAsString(list):
    return ", ".join(list)

def toString(dict):
    out = []
    for key in sorted(dict.keys()):
        out.append("{}: {}".format(key, dict[key]))
    return "; ".join(out)

##############
# Publication
##############
# list = [{u'affiliations': [u'Dept. of Nucl. Eng., Kyushu Univ., Fukuoka, Japan'], u'title': u'Electron field emission from polycrystalline silicon tips', u'controlledterms': [u'particle detectors', u'titanium', u'elemental semiconductors', u'torque', u'magnetic levitation', u'superconducting thin films'], u'authors': [u'Gallion, P.']}, {u'affiliations': [u'Key Laboratory of Materials for High Power Laser, Shanghai Institute of Optics and Fine Mechanics, Chinese Academy of Sciences, Shanghai 201800, People&#x2019', u'Dept. of Electr. & Electron. Eng., Middle East Tech. Univ., Ankara, Turkey'], u'title': u'On Communication Over Unknown Sparse Frequency-Selective Block-Fading Channels', u'controlledterms': [u'superconducting junction devices', u'elemental semiconductors', u'silicon', u'torque', u'magnetic flux', u'particle detectors', u'titanium', u'magnetic levitation'], u'authors': [u'Johnson, R.T.', u'Dickson, S.C.', u'Lee, A.', u'Matsumoto, Y.', u'Xu, Rongrong', u'Cabrera, B.']}, {u'affiliations': [u'Dept. of Nucl. Eng., Kyushu Univ., Fukuoka, Japan'], u'title': u'Heating of a dense plasma using a relativistic electron beam', u'controlledterms': [u'elemental semiconductors', u'superconducting junction devices', u'particle detectors', u'superconducting thin films', u'torque', u'magnetic levitation', u'neutrino detection and measurement', u'titanium'], u'authors': [u'Doisneau, B.', u'Tickle, R.', u'Das, A.', u'Bland, R.W.']}]
#mapped = ["{}; {}; {}".format(x['name'], x['title'], x['year']) for x in list]

##############
# Author
##############
# list = [{u'title': u'PhD', u'name': u'Stricker, D.A.', u'year': 1972}, {u'title': u'PhD', u'name': u'Anderson, C.C.', u'year': 1992}, {u'title': u'PhD', u'name': u'Bing, D.D.', u'year': 1955}, {u'title': u'PhD', u'name': u'Dignan, T.G.', u'year': 1985}, {u'title': u'PhD', u'name': u'Seneclauze, C.M.', u'year': 1964}, {u'title': u'PhD', u'name': u'Cabrera, B.', u'year': 1974}, {u'title': u'PhD', u'name': u'Takeno, K.', u'year': 1973}, {u'title': u'PhD', u'name': u'Katase, A.', u'year': 1988}, {u'title': u'PhD', u'name': u'Takada, S.', u'year': 1959}, {u'title': u'PhD', u'name': u'Shield, T.', u'year': 1982}, {u'title': u'PhD', u'name': u'Wuttig, M.', u'year': 1991}, {u'title': u'PhD', u'name': u'Bellet-Amalric, E.', u'year': 1964}, {u'title': u'PhD', u'name': u'Sarigiannidou, E.', u'year': 1975}, {u'title': u'PhD', u'name': u'Monroy, E.', u'year': 1969}, {u'title': u'PhD', u'name': u'Khurgin, J.', u'year': 1986}, {u'title': u'PhD', u'name': u'Hu, Lili', u'year': 1981}]
# list = [{u'affiliations': [u'Hewlett-Packard Lab., Palo Alto, CA, USA', u'Dept. of Phys. & Astron., San Francisco State Univ., CA, USA'], u'title': u'MOCVD <formula formulatype=inline> <img src=/images/tex/19426.gif alt=\\\\hbox {Ge}_{3}\\\\hbox {Sb}_{2}\\\\hbox {Te}_{5}> </formula> for PCM Applications', u'controlledterms': [u'elemental semiconductors', u'reluctance motors', u'particle detectors', u'magnetic levitation', u'torque', u'lattice phonons', u'superconducting thin films'], u'authors': [u'Lee, A.', u'Sarigiannidou, E.', u'Tozoni, O.V.']}, {u'affiliations': [u'Hewlett-Packard Lab., Palo Alto, CA, USA'], u'title': u'Optimization of the Scheduler for the Non-Blocking High-Capacity Router', u'controlledterms': [u'reluctance motors', u'particle detectors', u'scanning electron microscope examination of materials', u'superconducting junction devices', u'silicon', u'torque', u'superconductive tunnelling', u'neutrino detection and measurement'], u'authors': [u'Sarigiannidou, E.', u'Neuhauser, B.', u'Young, B.A.']}, {u'affiliations': [u'Hewlett-Packard Lab., Palo Alto, CA, USA', u'Dept. of Aerosp. Eng. & Mech., Minnesota Univ., Minneapolis, MN, USA'], u'title': u'Growth of epitaxial YbBa<inf>2</inf>Cu<inf>3</inf>O<inf>7</inf> superconductor by liquid&#x2010;gas&#x2010;solidification processing', u'controlledterms': [u'particle detectors', u'grain size', u'superconducting thin films', u'superconducting junction devices', u'reluctance motors', u'X-ray detection and measurement'], u'authors': [u'Ertan, H.B.', u'Cabrera, B.', u'Zhuangde Jiang', u'Monroy, E.', u'McVittie, J.P.', u'Sarigiannidou, E.']}, {u'affiliations': [u'Hewlett-Packard Lab., Palo Alto, CA, USA'], u'title': u'Improvement of the stability of high-voltage generators for perturbations within a frequency bandwidth of 0.03--1000 Hz', u'controlledterms': [u'scanning electron microscope examination of materials', u'magnetic levitation', u'torque', u'superconductive tunnelling', u'particle detectors', u'stepping motors', u'X-ray detection and measurement', u'superconducting junction devices'], u'authors': [u'Monroy, E.', u'Kokorin, V.V.', u'Stricker, D.A.', u'Tickle, R.', u'Dickson, S.C.', u'Sarigiannidou, E.']}, {u'affiliations': [u'Hewlett-Packard Lab., Palo Alto, CA, USA', u'CEA-Grenoble, INAC/SP2M/NPSC, 17 Rue des Martyrs, 38054 Grenoble cedex 9, France'], u'title': u'[Front cover]', u'controlledterms': [u'neutrino detection and measurement', u'superconductive tunnelling', u'titanium', u'particle detectors', u'superconducting junction devices', u'torque', u'scanning electron microscope examination of materials'], u'authors': [u'McVittie, J.P.', u'Sarigiannidou, E.']}, {u'affiliations': [u'Hewlett-Packard Lab., Palo Alto, CA, USA'], u'title': u'Microstructure and phase composition evolution of nano-crystalline carbon films: Dependence on deposition temperature', u'controlledterms': [u'superconductive tunnelling', u'torque', u'particle detectors', u'neutrino detection and measurement', u'superconducting junction devices', u'elemental semiconductors', u'superconducting thin films', u'reluctance motors'], u'authors': [u'Bing, D.D.', u'Neuhauser, B.', u'Sarigiannidou, E.']}, {u'affiliations': [u'Hewlett-Packard Lab., Palo Alto, CA, USA', u'Dept. of Phys., Stanford Univ., CA, USA'], u'title': u'Energies for atomic emissions from defect sites on the Si surfaces: The effects of halogen adsorbates', u'controlledterms': [u'neutrino detection and measurement', u'stepping motors', u'particle detectors', u'lattice phonons'], u'authors': [u'Sarigiannidou, E.']}]
list = [OrderedDict([(u'title', u'MOCVD <formula formulatype="inline"> <img src="/images/tex/19426.gif" alt="\\hbox {Ge}_{3}\\hbox {Sb}_{2}\\hbox {Te}_{5}"> </formula> for PCM Applications'), (u'authors', [u'Lee, A.', u'Sarigiannidou, E.', u'Tozoni, O.V.']), (u'affiliations', [u'Hewlett-Packard Lab., Palo Alto, CA, USA', u'Dept. of Phys. & Astron., San Francisco State Univ., CA, USA']), (u'controlledterms', [u'elemental semiconductors', u'reluctance motors', u'particle detectors', u'magnetic levitation', u'torque', u'lattice phonons', u'superconducting thin films'])]), OrderedDict([(u'title', u'Optimization of the Scheduler for the Non-Blocking High-Capacity Router'), (u'authors', [u'Sarigiannidou, E.', u'Neuhauser, B.', u'Young, B.A.']), (u'affiliations', [u'Hewlett-Packard Lab., Palo Alto, CA, USA']), (u'controlledterms', [u'reluctance motors', u'particle detectors', u'scanning electron microscope examination of materials', u'superconducting junction devices', u'silicon', u'torque', u'superconductive tunnelling', u'neutrino detection and measurement'])]), OrderedDict([(u'title', u'Growth of epitaxial YbBa<inf>2</inf>Cu<inf>3</inf>O<inf>7</inf> superconductor by liquid&#x2010;gas&#x2010;solidification processing'), (u'authors', [u'Ertan, H.B.', u'Cabrera, B.', u'Zhuangde Jiang', u'Monroy, E.', u'McVittie, J.P.', u'Sarigiannidou, E.']), (u'affiliations', [u'Hewlett-Packard Lab., Palo Alto, CA, USA', u'Dept. of Aerosp. Eng. & Mech., Minnesota Univ., Minneapolis, MN, USA']), (u'controlledterms', [u'particle detectors', u'grain size', u'superconducting thin films', u'superconducting junction devices', u'reluctance motors', u'X-ray detection and measurement'])]), OrderedDict([(u'title', u'Improvement of the stability of high-voltage generators for perturbations within a frequency bandwidth of 0.03--1000 Hz'), (u'authors', [u'Monroy, E.', u'Kokorin, V.V.', u'Stricker, D.A.', u'Tickle, R.', u'Dickson, S.C.', u'Sarigiannidou, E.']), (u'affiliations', [u'Hewlett-Packard Lab., Palo Alto, CA, USA']), (u'controlledterms', [u'scanning electron microscope examination of materials', u'magnetic levitation', u'torque', u'superconductive tunnelling', u'particle detectors', u'stepping motors', u'X-ray detection and measurement', u'superconducting junction devices'])]), OrderedDict([(u'title', u'[Front cover]'), (u'authors', [u'McVittie, J.P.', u'Sarigiannidou, E.']), (u'affiliations', [u'Hewlett-Packard Lab., Palo Alto, CA, USA', u'CEA-Grenoble, INAC/SP2M/NPSC, 17 Rue des Martyrs, 38054 Grenoble cedex 9, France']), (u'controlledterms', [u'neutrino detection and measurement', u'superconductive tunnelling', u'titanium', u'particle detectors', u'superconducting junction devices', u'torque', u'scanning electron microscope examination of materials'])]), OrderedDict([(u'title', u'Microstructure and phase composition evolution of nano-crystalline carbon films: Dependence on deposition temperature'), (u'authors', [u'Bing, D.D.', u'Neuhauser, B.', u'Sarigiannidou, E.']), (u'affiliations', [u'Hewlett-Packard Lab., Palo Alto, CA, USA']), (u'controlledterms', [u'superconductive tunnelling', u'torque', u'particle detectors', u'neutrino detection and measurement', u'superconducting junction devices', u'elemental semiconductors', u'superconducting thin films', u'reluctance motors'])]), OrderedDict([(u'title', u'Energies for atomic emissions from defect sites on the Si surfaces: The effects of halogen adsorbates'), (u'authors', [u'Sarigiannidou, E.']), (u'affiliations', [u'Hewlett-Packard Lab., Palo Alto, CA, USA', u'Dept. of Phys., Stanford Univ., CA, USA']), (u'controlledterms', [u'neutrino detection and measurement', u'stepping motors', u'particle detectors', u'lattice phonons'])])]
mapped = ["{}; {}; {}; {}".format(x['title'], listAsString(x['authors']), listAsString(x['affiliations']), listAsString(x['controlledterms'])) for x in list]

##############
# Custom types
##############
# publications
# orderedDict = [OrderedDict([(u'name', u'Neuhauser, B.'), (u'title', u'professor'), (u'year', 1973)]), OrderedDict([(u'name', u'Takeno, K.'), (u'title', u'PhD'), (u'year', 1973)])]
# orderedDict = [OrderedDict([(u'nom', u'Neuhauser, B.'), (u'titre', u'professor'), (u'annee', 1973)]), OrderedDict([(u'nom', u'Takeno, K.'), (u'titre', u'PhD'), (u'annee', 1973)])]
# mapped = [toString(x) for x in orderedDict]

mapped.sort()
print "\n".join(mapped)