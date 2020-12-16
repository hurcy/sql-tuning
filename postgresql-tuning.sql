

select count(1) from synthea_cdm.concept; --  7,403,692
select count(1) from synthea_cdm.drug_exposure; -- 148,406,087
select count(1) from synthea_cdm.visit_occurrence; -- 133,016,052
select count(1) from synthea_cdm.condition_occurrence; -- 35,885,806


--- 불필요한 Loop 제거
drop procedure C_NAME(l_concept_name varchar, cid int);
create or replace
procedure C_NAME(l_concept_name varchar, cid int) language plpgsql as $$ 

begin
-- condition_occurrence 테이 내용으로 100000번 반복 LOOP 수행
 for cid in (
select
    co.condition_concept_id
from
    synthea_cdm.condition_occurrence co
limit 100000) loop begin
-- 컨셉명 확인 위해 prod_id로 반복 조회
 select
    c.concept_name
into
    l_concept_name
from
    synthea_cdm.concept c
where
    c.concept_id = cid;
end;

raise notice '컨셉명:% ',
l_concept_name;
end loop;
end;

$$;

call C_NAME('public.procedure_test_1', 260139);

drop procedure C_NAME2(l_concept_name varchar, cname varchar);
create or replace
procedure C_NAME2(l_concept_name varchar, cname varchar) language plpgsql as $$ 

begin
-- Loop 안의 sql을 스칼라 서브쿼리로 변경
 for cname in (
select
    (
    select
        c.concept_name
    from
        synthea_cdm.concept c
    where
        c.concept_id = co.condition_concept_id) as concept_name
from
    synthea_cdm.condition_occurrence co
limit 100000) 

loop
-- 컨셉명 확인 위해 prod_id로 반복 조회
 raise notice '컨셉명:% ',
cname;
end loop;
end;

$$;

call C_NAME2('public.procedure_test_1', '');

--- 간단한 로직은 함수 대신 조인으로 구현
drop function f_get_concept_name(cid int);
CREATE OR REPLACE FUNCTION 
    f_get_concept_name(cid int) 
RETURNS 
TABLE (cname varchar(255))
AS $$
BEGIN
    RETURN QUERY execute (
        'SELECT concept_name FROM synthea_cdm.concept WHERE concept_id = ' || cid );
END; $$
LANGUAGE PLPGSQL;

select f_get_concept_name(9301);


SELECT co.condition_occurrence_id, f_get_concept_name(co.condition_concept_id ) AS concept_name
FROM synthea_cdm.condition_occurrence co
WHERE co.condition_occurrence_id BETWEEN 1 AND 1000000;

SELECT co.condition_occurrence_id, c.concept_name 
FROM synthea_cdm.condition_occurrence co, synthea_cdm.concept c 
WHERE co.condition_concept_id = c.concept_id and co.condition_occurrence_id BETWEEN 1 AND 1000000;

--- 함수 수행 횟수 감소
explain select co.condition_occurrence_id, c.concept_name 
FROM synthea_cdm.condition_occurrence co, synthea_cdm.concept c 
WHERE co.condition_concept_id = c.concept_id 
	and co.condition_occurrence_id BETWEEN 1 AND 100000 
	and (select f_get_concept_name(co.condition_concept_id )) = 'Fever';


explain select condition_occurrence_id , concept_name
from (
	select co.condition_occurrence_id, f_get_concept_name(co.condition_concept_id ) as concept_name
	FROM synthea_cdm.condition_occurrence co, synthea_cdm.concept c 
	WHERE co.condition_concept_id = c.concept_id 
		and co.condition_occurrence_id BETWEEN 1 AND 100000 
) t
where concept_name = 'Fever'; 

-----LATERVAL View를 이용한 성능 개선
-- 217 ms
select
	A.*,
	B.*
from
	synthea_cdm.concept A,
	(
	select
		drug_concept_id, AVG(days_supply)
	from
		synthea_cdm.drug_exposure de
	where
		de.drug_exposure_id < 800000
	group by
		drug_concept_id ) B
where
	A.concept_id = B.drug_concept_id
	and A.concept_name = 'simvastatin 20 MG Oral Tablet';

-- 5,119 ms
select
	A.*,
	B.*
from
	synthea_cdm.concept A,
	lateral (
	select
		drug_concept_id, AVG(days_supply)
	from
		synthea_cdm.drug_exposure B
	where
		B.drug_exposure_id < 800000
		and B.drug_concept_id = A.concept_id
	group by
		drug_concept_id) B
where
	A.concept_name = 'simvastatin 20 MG Oral Tablet';

-----중복테이블 제거


select
	c.concept_id ,
	de.drug_exposure_start_date ,
	de.days_supply 
from
	synthea_cdm.concept c,
	synthea_cdm.drug_exposure de
where
	c.concept_id = de.drug_concept_id
	and c.vocabulary_id = 'RxNorm'
union all
select
	c.concept_id ,
	de.drug_exposure_start_date ,
	de.days_supply 
from
	synthea_cdm.concept c,
	synthea_cdm.drug_exposure de
where
	c.concept_id = de.drug_concept_id
	and c.domain_id = 'Drug';

select
	c.concept_id ,
	de.drug_exposure_start_date ,
	de.days_supply 
from
	(
	select
		c.concept_id
	from
		synthea_cdm.concept c
	where
		c.vocabulary_id = 'RxNorm'
union all
	select
		c.concept_id
	from
		synthea_cdm.concept c
	where
		c.domain_id = 'Drug' ) c,
	synthea_cdm.drug_exposure de
where
	c.concept_id = de.drug_concept_id;


-------스칼라서브쿼리를 조인으로 변경한 성능 개선

explain select
	de.drug_exposure_id ,
	(
	select
		concept_name
	from
		synthea_cdm.concept
	where
		concept_id = de.drug_concept_id ) as concept_name
from
	synthea_cdm.drug_exposure de
where
	de.drug_exposure_id between 1 and 10000000;

explain select
	de.drug_exposure_id ,
	c.concept_name
from
	synthea_cdm.drug_exposure de
left join synthea_cdm.concept c on
	c.concept_id = de.drug_concept_id
where
	de.drug_exposure_id between 1 and 10000000;

----페이징 쿼리 최적화

-- 부분 페이징 처리 방식(1)
-- 420 ms

CREATE INDEX DRUG_EXPOSURE_X02 ON synthea_cdm.drug_exposure(days_supply);

select
	drug_exposure_id,
	drug_concept_id,
	days_supply
from
	synthea_cdm.drug_exposure de
where
	drug_concept_id = 40213216
order by
	days_supply desc,
	drug_exposure_id desc offset 0 rows fetch next 10 rows only;

-- 801 ms
select
	de.drug_exposure_id,
	de.drug_concept_id,
	de.days_supply
from
	(
	select
		days_supply
	from
		(
		select
			days_supply
		from
			synthea_cdm.drug_exposure
		where
			drug_concept_id = 40213216
		order by
			days_supply desc offset 0 rows fetch next 10 rows only ) A
	group by
		days_supply ) B,
	synthea_cdm.drug_exposure de
where
	B.days_supply = de.days_supply
	and de.drug_concept_id = 40213216
order by
	de.days_supply desc,
	de.drug_exposure_id desc offset 0 rows fetch next 10 rows only;

-- 부분 페이징 처리 방식(2)
-- 1m 56s
-- vo집합 133,016,052 건과 de집합 148,406,087 건을 조인하고, Sorting 후에 20건을 추출한다.

select
	*
from
	synthea_cdm.visit_occurrence vo,
	synthea_cdm.drug_exposure de
where
	vo.visit_occurrence_id = de.visit_occurrence_id
	and vo.visit_start_date < '2009-11-15'
order by
	vo.visit_start_date desc,
	de.days_supply offset 10 rows fetch next 10 rows only;

-- 1m 28s
-- K 인라인 뷰에서 20 로우의 visit_start_date 추출하고 중복값을 제거하기 위해 GROUP BY를 수행한다.(1건 추출)
-- 해당 visit_start_date 값으로 visit_occurrence 테이블을 조회한다. K 인라인 뷰로부터 20 개 이내의 visit_start_date 값을 제공 받으므로 건수가 매우 적다.(2739건)
-- drug_exposure과 조인을 수행하여 최종 데이터를 추출하며(27390건) vo.visit_start_date DESC, de.days_supply 로 정렬을 수행한다.
-- 최종 10건의 데이터를 출력한다.

select
	*
from
	(
	select
		visit_start_date
	from
		(
		select
			visit_start_date
		from
			synthea_cdm.visit_occurrence vo, synthea_cdm.drug_exposure de
		where
			vo.visit_occurrence_id = de.visit_occurrence_id
			and vo.visit_start_date < '2009-11-15'
		order by
			vo.visit_start_date desc offset 0 rows fetch next 20 rows only ) K -- 1m 14s
	group by
		visit_start_date ) V, -- 1m 14s
	synthea_cdm.visit_occurrence vo,
	synthea_cdm.drug_exposure de
where
	V.visit_start_date = vo.visit_start_date 
	and vo.visit_occurrence_id = de.visit_occurrence_id
	and vo.visit_start_date < '2009-11-15'
order by
	vo.visit_start_date desc,
	de.days_supply offset 10 rows fetch next 10 rows only;

--- union all 을 이용한 페이징 처리
-- 49 s
-- UNION ALL 위쪽 집합과 아래 집합 결과를 합한 후 SORTING하여 10건을 추출한다.
select
	*
from
	(
	select
		visit_occurrence_id, visit_concept_id as concept_id
	from
		synthea_cdm.visit_occurrence
	where
		visit_occurrence_id > 5000
union all
	select
		visit_occurrence_id, drug_concept_id as concept_id
	from
		synthea_cdm.drug_exposure
	where
		visit_occurrence_id > 5000 ) A
order by
	visit_occurrence_id desc offset 10 rows fetch next 10 rows only;

-- 20 ms
-- 위쪽 집합에서 20건, 아래쪽 집합에서 20건을 추출하여 합친 후 다시 Sorting을 하고 10건을 추출한다.
select
	visit_occurrence_id,
	concept_id
from
	(
	select
		*
	from
		(
		select
			visit_occurrence_id, visit_concept_id as concept_id
		from
			synthea_cdm.visit_occurrence
		where
			visit_occurrence_id > 5000
		order by
			visit_occurrence_id desc offset 0 rows fetch next 20 rows only ) A
union all
	select
		*
	from
		(
		select
			visit_occurrence_id, drug_concept_id as concept_id
		from
			synthea_cdm.drug_exposure
		where
			visit_occurrence_id > 5000
		order by
			visit_occurrence_id desc offset 0 rows fetch next 20 rows only ) B ) C
order by
	visit_occurrence_id desc offset 10 rows fetch next 10 rows only;

--- 인라인 뷰를 이용한 페이징 처리
-- 1m 14s
-- Limit  (cost=16723301.07..16723385.70 rows=10 width=528)
explain select
	A.visit_occurrence_id,
	A.visit_start_date,
	C.drug_concept_id ,
	(
	select
		B.concept_name 
	from
		synthea_cdm.concept B
	where
		B.concept_id = C.drug_concept_id) as concept_name
from
	synthea_cdm.visit_occurrence A,
	synthea_cdm.drug_exposure C
where
	A.visit_occurrence_id = C.visit_occurrence_id
	and C.days_supply < 20
order by
	A.visit_occurrence_id,
	A.visit_start_date offset 40000 rows fetch next 10 rows only;

-- 1m 13s
-- Subquery Scan on v  (cost=7605378.94..7605464.74 rows=10 width=528)
explain select
	V.visit_occurrence_id,
	V.visit_start_date,
	V.drug_concept_id,
	(
	select
		B.concept_name
	from
		synthea_cdm.concept B
	where
		B.concept_id = V.drug_concept_id) as concept_name
from
	(
	select
		A.visit_occurrence_id , A.visit_start_date, C.drug_concept_id
	from
		synthea_cdm.visit_occurrence A, synthea_cdm.drug_exposure C
	where
		A.visit_occurrence_id = C.visit_occurrence_id
		and C.days_supply < 20
	order by
		A.visit_occurrence_id, A.visit_start_date offset 40000 rows fetch next 10 rows only ) V;

--- OUTER 조인을 이용한 페이징 처리

-- 5.7s
-- 단지 B.visit_start_date 를 출력하기 위한 용도로 outer join 수행
-- OUTER 조인에서는 A집합 전체와 B 집합이 조인을 수행
-- Limit  (cost=15541.04..15544.67 rows=10 width=20)
select
	A.drug_type_concept_id,
	A.drug_exposure_id,
	A.drug_concept_id,
	B.visit_start_date
from
	synthea_cdm.drug_exposure A
left join visit_occurrence B on
	A.visit_occurrence_id = B.visit_occurrence_id
order by
	A.days_supply desc offset 40000 rows fetch next 10 rows only;


-- 57ms
-- A집합 에서 10건만 추출하여 B 집합과 조인
-- Nested Loop Left Join  (cost=3510.15..3596.41 rows=10 width=20)
select
	V.drug_exposure_id,
	V.visit_occurrence_id,
	V.drug_concept_id,
	B.visit_start_date
from (
select
	A.visit_occurrence_id,
	A.drug_exposure_id,
	A.drug_concept_id,
	A.days_supply
from
	synthea_cdm.drug_exposure A
	order by
	A.days_supply desc offset 40000 rows fetch next 10 rows only
) V 
left join visit_occurrence B on
	V.visit_occurrence_id = B.visit_occurrence_id
order by V.days_supply desc;


--- 웹화면 페이징 쿼리(1)

-- 첫번째 페이지네이션 10개를 계산하기 위한 SQL
--CNT < 2 이면, <다음> 콤보 박스를 INACTIVE로 변경
--CNT >= 2 이면, <다음> 콤보박스를 ACTIVE로 변경
--사용자가 <다음> 을 클릭하면 다음 101 ~ 200 ROW 에 대해서 동일한 패턴의 쿼리 수행

-- 12ms
select ceil(count(*)/ 100) cnt
from synthea_cdm.visit_occurrence vo 
where vo.person_id < 100;

-- 14ms
-- 첫번째 페이지네이션 10개용 출력 = 100건
select visit_occurrence_id, person_id, visit_start_date
from synthea_cdm.visit_occurrence
where person_id < 100
order by visit_start_date desc 
offset 0 rows fetch next 100 rows only;

-- 14ms
-- 101건의 데이터를 추출하여 100건은 화면에 추출하며 마지막 101번째의 데이터는 2번째 페이지가 존재한다는 의미로 사용
-- 이와 같이 수행을 계속하여 마지막에 추출되는 데이터가 101건을 만족시키지 못하는 경우 해당 화면은 마지막 페이지
select visit_occurrence_id, person_id, visit_start_date
from synthea_cdm.visit_occurrence
where person_id < 100
order by visit_start_date desc 
offset 0 rows fetch next 101 rows only;

--- 웹화면 페이징 쿼리(3)

-- 654 ms
-- window 함수 2개 사용
select
	*
from
	(
	select
		count(*) over () as CNT, 
		row_number () over (order by D.visit_occurrence_id, D.drug_concept_id) as RNUM, 
		D.drug_exposure_id, 
		D.visit_occurrence_id, 
		D.drug_concept_id
	from
		synthea_cdm.drug_exposure D, synthea_cdm.visit_occurrence O
	where
		D.visit_occurrence_id = O.visit_occurrence_id
		and D.visit_occurrence_id between 1000 and 100000 ) A
where
	RNUM between 21 and 30;

-- 312 ms
-- window 함수 1개 사용

select
	*
from
	(
	select
		count(*) over () as CNT,  
		D.drug_exposure_id, 
		D.visit_occurrence_id, 
		D.drug_concept_id
	from
		synthea_cdm.drug_exposure D, synthea_cdm.visit_occurrence O
	where
		D.visit_occurrence_id = O.visit_occurrence_id
		and D.visit_occurrence_id between 1000 and 100000 
	order by D.visit_occurrence_id, D.drug_concept_id ) A
offset 20 rows fetch next 10 rows only;

--- 상관 서브쿼리

-- 10.17s
-- 서브쿼리 내에 메인 쿼리와의 조인절이 있다.
-- drug_exposure 을 2번 액세스 하는 비효율이 있다.
select
	D.visit_occurrence_id,
	D.days_supply,
	O.person_id
from
	synthea_cdm.drug_exposure D,
	synthea_cdm.visit_occurrence O
where
	D.visit_occurrence_id = O.visit_concept_id
	and D.visit_occurrence_id = (
	select
		max(visit_occurrence_id)
	from
		synthea_cdm.drug_exposure D
	where
		D.visit_occurrence_id = O.visit_concept_id )
	and D.days_supply > 10;

-- 5m 49s
-- WINDOW FUNCTION을 사용해서 drug_exposure을 1회만 액세스한다... 그러나 실행이 빠르지 않음.
select visit_occurrence_id, days_supply, person_id 
from
	(
	select
		D.visit_occurrence_id, D.days_supply, O.person_id,
		case
			D.visit_occurrence_id when max(D.visit_occurrence_id) over (partition by D.visit_occurrence_id) then 'X'
		end M_visit_occurrence_id
	from
		synthea_cdm.drug_exposure D, synthea_cdm.visit_occurrence O
	where
		D.visit_occurrence_id = O.visit_concept_id 
	and D.days_supply > 10 ) Z
where
	M_visit_occurrence_id is not null;

--- 비상관 서브쿼리
-- 1m 28s
with v as (
select
	visit_occurrence_id , sum(days_supply) as days_supply
from
	synthea_cdm.drug_exposure D
where
	D.drug_type_concept_id = 38000177
group by
	visit_occurrence_id )
select
	O.visit_occurrence_id,
	v.days_supply
from
	synthea_cdm.visit_occurrence O,
	v
where
	O.visit_occurrence_id = v.visit_occurrence_id
	and v.days_supply = (
	select
		max(v.days_supply)
	from
		v);
-- 1m 24s
-- WINDOW FUNCTION을 사용해서 drug_exposure을 1회만 액세스한다.
select
	O.visit_occurrence_id,
	v.days_supply
from
	synthea_cdm.visit_occurrence O,
	(
	select
		visit_occurrence_id, sum(days_supply) as days_supply , max(sum(days_supply)) over () max_s
	from
		synthea_cdm.drug_exposure de
	where
		drug_type_concept_id = 38000177
	group by
		visit_occurrence_id ) v
where
	O.visit_occurrence_id = v.visit_occurrence_id
	and v.days_supply = v.max_s;


--- NOT IN 절 개선

-- 17.55s
-- NOT IN 절은 subquery collapse가 동작하지 않는다.. 라고 했으나, postgresql 버전이 달라서인지 join이 되었다.
-- Hash Join  (cost=5203122.31..11200746.08 rows=84804464 width=8)
--   Hash Cond: (de.visit_occurrence_id = vo.visit_occurrence_id)
select
	de.drug_exposure_id,
	de.days_supply
from
	synthea_cdm.drug_exposure de
where
	de.visit_occurrence_id in (
	select
		visit_occurrence_id
	from
		synthea_cdm.visit_occurrence vo
	where
		visit_start_date > '2010-01-01');

	
-- 17.61s
-- 성능이 좋지 않을 경우 NOT EXISTS 절을 사용하여 Anti Join을 유도한다.
select
	de.drug_exposure_id,
	de.days_supply
from
	synthea_cdm.drug_exposure de
where
	not exists (
	select
		1
	from
		synthea_cdm.visit_occurrence vo
	where
		vo.visit_occurrence_id = de.visit_occurrence_id
		and visit_start_date > '2010-01-01');
	
--- IN 절 개선

-- 364ms
select
	visit_occurrence_id,
	person_id
	drug_exposure_start_date,
	days_supply
from
	synthea_cdm.drug_exposure de2
where
	drug_exposure_start_date in (
	select
		max(drug_exposure_start_date)
	from
		synthea_cdm.drug_exposure de
		where de.person_id = de2.person_id 
	)
	and
		de2.visit_occurrence_id between 0 and 36827723;

-- 25.7s
select
	de2.visit_occurrence_id,
	de2.person_id,
	de2.drug_exposure_start_date,
	de2.days_supply
from
	synthea_cdm.drug_exposure de2,
	(
	select
		person_id, max(drug_exposure_start_date) as m_drug_exposure_start_date
	from
		synthea_cdm.drug_exposure de
	where
		de.visit_occurrence_id between 0 and 36827723
	group by
		person_id) de
where
	de2.person_id = de.person_id
	and de2.drug_exposure_start_date = de.m_drug_exposure_start_date
	and de2.visit_occurrence_id between 0 and 36827723;


--- 서브쿼리를 조인으로 변경

-- 57ms
select
	vo.person_id,
	vo.admitting_source_value
from
	synthea_cdm.visit_occurrence vo
where
	vo.visit_start_date > '2015-01-01'
	and exists (
	select
		1
	from
		synthea_cdm.drug_exposure de
	where
		vo.visit_occurrence_id = de.visit_occurrence_id
	group by
		de.visit_occurrence_id
	having
		sum(de.days_supply) > 100 );

-- 1m 19s
select
	vo.person_id,
	vo.admitting_source_value
from
	synthea_cdm.visit_occurrence vo,
	synthea_cdm.drug_exposure de
where
	vo.visit_occurrence_id = de.visit_occurrence_id
	and vo.visit_start_date > '2015-01-01'
group by
	vo.visit_occurrence_id
having
	sum(de.days_supply) > 100;

--- 조인 방법 개선


-- postgrsql 버전업하면서 개선된걸까? 개전 전/후 쿼리 플래닝이 동일함.
-- 22ms
select
	O.visit_occurrence_id,
	O.visit_start_date,
	O.visit_concept_id
from
	synthea_cdm.visit_occurrence O
where
	O.person_id = 872627
	and visit_occurrence_id > 0
	and not exists (
	select
		1
	from
		synthea_cdm.drug_exposure D, synthea_cdm.concept P
	where
		D.visit_occurrence_id = O.visit_occurrence_id
		and D.drug_concept_id = P.concept_id
		and D.drug_exposure_id between 46353623 and 55627471);

/*+ NestLoop(o d p) */
select
	O.visit_occurrence_id,
	O.visit_start_date,
	O.visit_concept_id
from
	synthea_cdm.visit_occurrence O
where
	O.person_id = 872627
	and visit_occurrence_id > 0
	and not exists (
	select
		1
	from
		synthea_cdm.drug_exposure D, synthea_cdm.concept P
	where
		D.visit_occurrence_id = O.visit_occurrence_id
		and D.drug_concept_id = P.concept_id
		and D.drug_exposure_id between 46353623 and 55627471);

--- GROUPING SETS 개선

-- 42.55s
-- 테이블 전체를 읽은 후, person_id, drug_exposure_start_date 각각 GROUPING 한다.
select
	person_id,
	drug_exposure_start_date,
	avg(days_supply) as avg_supply
from
	synthea_cdm.drug_exposure
group by
	grouping sets (person_id, drug_exposure_start_date);

-- 54.28s
-- (person_id, drug_exposure_start_date)로 GROUP BY하여 건수를 줄인 상태에서 person_id, drug_exposure_start_date 각각 GROUPING한다.
select
	person_id,
	drug_exposure_start_date,
	sum(s_supply)/ sum(cnt) as avg_supply
from
	(
	select
		person_id, drug_exposure_start_date, sum(days_supply) as s_supply, count(days_supply) as cnt
	from
		synthea_cdm.drug_exposure
	group by
		person_id , drug_exposure_start_date ) A
group by
	grouping sets(person_id, drug_exposure_start_date );

--- Group By Placement

-- 18.74s
select
	P.concept_name,
	sum(D.days_supply)
from
	synthea_cdm.concept P,
	synthea_cdm.drug_exposure D
where
	P.concept_id = D.drug_concept_id
	and D.drug_concept_id in (40163924, 19030765)
group by
	concept_name ;

-- 14.58s
-- 조인을 수행하기 전에 Group By를 먼저 수행하여 건수를 줄인 후 조인을 수행함으로서, 조인 건수를 획기적으로 감소시킨다.
-- OLTP 보다는 DW의 대용량 시스템에서 사용할 경우 성능 향상을 극대화 할 수 있다.
select
	P.concept_name,
	sum(D.days_supply)
from
	synthea_cdm.concept P,
	(
	select
		drug_concept_id, sum(days_supply) as days_supply
	from
		synthea_cdm.drug_exposure
	where
		drug_concept_id in (40163924, 19030765)
	group by
		drug_concept_id ) D
where
	P.concept_id = D.drug_concept_id
group by
	concept_name ;

--- 대용량 테이블 집계

-- 23.7s
-- 아래 쿼리는 visit_start_date 컬럼에 BTREE INDEX 있어도 건수가 많아서 속도향상이 안된다.
-- visit_start_date 년월별로 파티션을 구성하면 FULL SCAN 범위를 줄일 수 있다.

select person_id, count(*)
from synthea_cdm.visit_occurrence vo 
where visit_start_date between '1910-01-21' and '2020-06-30'
group by person_id;

-- 
-- PostgreSQL의 Block Range Index를 구성해도 파티션과 유사한 성능을 확보할 수 있다...고 하였으나. 실제 실험결과는 아니었음.
create index visit_occurrence_x01 on synthea_cdm.visit_occurrence  using brin(visit_start_date);

-- 23.8s
select person_id, count(*)
from synthea_cdm.visit_occurrence vo 
where visit_start_date between '1910-01-21' and '2020-06-30'
group by person_id;

drop index visit_occurrence_x01;

--- HashAggregate 유도

-- 1m 3s
-- drug_exposure_start_date별로 days_supply가 가장 큰 처방정보를 출력하는 쿼리이다.
select
	drug_exposure_start_date,
	days_supply,
	person_id
from
	(
	select
		row_number() over (partition by drug_exposure_start_date
	order by
		days_supply desc) as RN , days_supply, drug_exposure_start_date, person_id
	from
		synthea_cdm.drug_exposure de ) A
where
	RN = 1;

-- 43.3s
-- drug_exposure_start_date 를 기준으로 HashAggregate 수행하여 작업대상 건수를 줄인 후 Sorting을 수행한다.
select
	B.drug_exposure_start_date,
	B.days_supply,
	B.person_id
from
	(
	select
		drug_exposure_start_date, 
		(max(array[days_supply, drug_exposure_id ]))[2] as drug_exposure_id
	from
		synthea_cdm.drug_exposure
	group by
		drug_exposure_start_date) A
join synthea_cdm.drug_exposure B on
	B.drug_exposure_id = A.drug_exposure_id;


--- 인덱스 액세스 범위 최소화

CREATE INDEX VISIT_OCCURRENCE_X02 ON synthea_cdm.visit_occurrence(visit_start_date, person_id);

-- 6.18s
select
	count(*)
from
	synthea_cdm.visit_occurrence vo
where
	person_id = 872742
	and visit_start_date between '2010-01-01' and '2010-12-31';

-- 6.49
select
	count(*)
from
	synthea_cdm.visit_occurrence vo
where
	person_id = 872742
	and visit_start_date in (
	select
		DATE '20100101'- 1 + generate_series(1, DATE '20101231'-'20100101' + 1));

drop INDEX VISIT_OCCURRENCE_X02;

----OR 조건을 UNION 으로 변경

SELECT de.drug_exposure_id ,
	c.concept_name
FROM synthea_cdm.concept c, synthea_cdm.drug_exposure de
WHERE c.concept_id = de.drug_concept_id
AND (de.drug_concept_id in (40213216,40163924)
OR de.drug_source_value in ('314231', '2123111'));


SELECT de.drug_exposure_id ,
	c.concept_name
FROM synthea_cdm.concept c, synthea_cdm.drug_exposure de
WHERE c.concept_id = de.drug_concept_id
AND de.drug_concept_id in (40213216,40163924)
union
SELECT de.drug_exposure_id ,
	c.concept_name
FROM synthea_cdm.concept c, synthea_cdm.drug_exposure de
WHERE c.concept_id = de.drug_concept_id
AND de.drug_source_value in ('314231', '2123111');
