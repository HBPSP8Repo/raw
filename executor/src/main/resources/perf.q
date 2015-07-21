count(publications)

count(authors)

select title, count(partition) as n
from authors A
group by title: A.title

select title,
       (select A from partition) as people
from authors A
group by title: A.title

select title,
       (select year from partition) as values
from authors A
group by title: A.title

select * from (
    select article: P,
           (select A
            from P.authors a,
                 authors A
            where A.name = a
                  and A.title = "professor") as profs
    from publications P
    where "particle detectors" in P.controlledterms
          and "Stricker, D.A." in P.authors
    ) T having count(T.profs) > 0

select * from (select article.title, min(article.phd) as mphd, max(article.profs) as mprofs, count(article.phd) as cphd, count(article.profs) as cprofs from (
    select doc.p.title as title,
        (select p.year from doc.people p where p.title = "PhD") as phd,
        (select p.year from doc.people p where p.title = "professor") as profs
            from (
                select p, (select A from p.authors a, authors A where A.name = a) as people
                from publications p
                ) doc
                ) article) X where X.cphd > 0 and X.cprofs > 0 and X.mphd < X.mprofs
