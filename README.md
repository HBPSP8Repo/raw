raw
===

TODO:
- Add support for "group by" and "order by" (see related paper)
- Implement reduction to canonical forms (described in [1] page 19) ? 
  Is it necessary for normalization to work property? Re-read paper.
- Implement "graph reduction techniques" to prevent re-evaluating expensive expressions when normalizing (described in [1] page 19)
- Add support for "irrefutable patterns" in generators: how is it related w/ early/late loading?
- "Mapping" functions for "richer" data types/converting info between sources
- Caching policies
- DML language
- Built-in generator to query internal catalogs
- Repl should keep global context, remembering variables across queries
- Test query Section 5.4 from "Essence of LINQ" paper

REFERENCES:
[1] "Optimizing Object Queries using an Effective Calculus", Leonidas Fegaras, David Maier, TODS 2000
[2] ... order by / group by paper?
