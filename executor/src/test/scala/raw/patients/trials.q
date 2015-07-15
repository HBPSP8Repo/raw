select G, count(partition) from patients P group by G: struct(a:P.city, b:P.country)

--

select distinct I.gender, (select distinct country, count(partition) from I.people P group by country:P.country) as G from (
select distinct gender, (select P from partition) as people from patients P group by gender: P.gender
) I

--

select distinct T.gender, (select city, (select distinct patient_id from partition) as c from T.people P group by city: P.city) as X from
(select distinct gender, (select p from partition) as people from patients p group by gender: p.gender) T