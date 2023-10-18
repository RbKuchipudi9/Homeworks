#1
SELECT ad.animal_type, COUNT(DISTINCT af.animalkey) as num_animals
FROM animaldim ad
JOIN animalfact af ON ad.animalkey = af.animalkey
GROUP BY ad.animal_type;


#2
SELECT COUNT(*) as num_animals_with_multiple_outcomes
FROM (
    SELECT af.animalkey
    FROM animalfact af
    GROUP BY af.animalkey
    HAVING COUNT(*) > 1
) AS subquery;

#3



SELECT EXTRACT(MONTH FROM td.date_recorded) AS calendar_month, COUNT(af.outcomekey) AS num_outcomes
FROM animalfact af
JOIN timedim td ON af.timekey = td.timekey
GROUP BY calendar_month
ORDER BY num_outcomes DESC
LIMIT 5;


#4


select
	cat_age_grp,
	COUNT(*) as count
from
	(
	select
		ad.animalkey,
		case
			when AGE(ad.dofb) < interval '1 year' then 'Kitten'
			when AGE(ad.dofb) >= interval '1 year'
			and AGE(ad.dofb) <= interval '10 years' then 'Adult'
			when AGE(ad.dofb) > interval '10 years' then 'Senior'
			else 'Unknown'
		end as cat_age_grp
	from
		animaldim ad
) as cat_agegroup
join animalfact of2 on
	cat_agegroup.animalkey = of2.animalkey 
join outcomedim otd on
	of2.outcomekey  = otd.outcomekey 
where
	otd.outcome_type  = 'Adoption'
group by
	cat_age_grp;



#5


WITH OutcomeCumulative AS (
    SELECT
        td.date_recorded,
        COUNT(*) OVER (ORDER BY td.date_recorded) AS cumulative_total
    FROM
        animalfact af
    JOIN
        timedim td ON af.timekey = td.timekey
)

SELECT
    date_recorded,
    cumulative_total
FROM
    OutcomeCumulative;




