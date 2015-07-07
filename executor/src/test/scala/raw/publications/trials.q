select author as a1, (select distinct title as t1, affiliations as aff from partition) as articles
from publications P, P.authors A
where A = "Akoh, H."
group by author: A

val expected = ""