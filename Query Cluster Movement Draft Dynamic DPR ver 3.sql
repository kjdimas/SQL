DECLARE @cols AS NVARCHAR(MAX),
    @query_uc  AS NVARCHAR(MAX),
	@query_gram  AS NVARCHAR(MAX),
	@query_last_submit_date  AS NVARCHAR(MAX),
	@date datetime
	;

SET @date = '2021-06-01'

SET @cols = STUFF((SELECT distinct ',' + QUOTENAME(CAST(YEAR([Date]) as nvarchar) + '-' + RIGHT('0' + CAST(MONTH([Date]) as nvarchar),2)) 
            FROM db_analytic_dancow.dbo.T_Week  c
			WHERE
				c.[Date] < @date
            FOR XML PATH(''), TYPE
            ).value('.', 'NVARCHAR(MAX)') 
        ,1,1,'')


DROP TABLE IF EXISTS cluster_movement_uc
DROP TABLE IF EXISTS cluster_movement_gram
DROP TABLE IF EXISTS cluster_movement_last_submit_date
DROP TABLE IF EXISTS cluster_movement_temp
DROP TABLE IF EXISTS cluster_movement_temp_2
DROP TABLE IF EXISTS cluster_movement
DROP TABLE IF EXISTS point_issued_dancow

SELECT distinct member_id, product_id, unique_code, channel, brand, product, retailer, grammage, point, point_product, created_at, 
	remarks, CAST(YEAR(created_at) as nvarchar) + '-' + RIGHT('0' + CAST(MONTH(created_at) as nvarchar), 2) as months
INTO point_issued_dancow
FROM dpr_point_issued
WHERE brand = 'dancow'


SET @query_uc =
'
SELECT
	*
INTO cluster_movement_uc
FROM
	(
	SELECT
		b.id as member_id, b.created_at, b.flag, b.status_member, a.months, a.unique_code
	FROM
		db_analytic_dancow.dbo.point_issued_dancow	a
	RIGHT JOIN
		db_analytic_dancow.dbo.dpr_submission_cluster_21_05 b ON a.member_id = b.id
	) x
PIVOT
	( COUNT(unique_code) FOR months IN (' + @cols + ')
	) p
UNPIVOT
	( unique_code FOR months IN (' + @cols + ')
	) u
'

execute(@query_uc)

SET @query_gram =
'
SELECT
	*
INTO cluster_movement_gram
FROM
	(
	SELECT
		b.id as member_id, a.months, a.grammage
	FROM
		db_analytic_dancow.dbo.point_issued_dancow	a
	RIGHT JOIN
		db_analytic_dancow.dbo.dpr_submission_cluster_21_05 b ON a.member_id = b.id
	) x
PIVOT
	( SUM(grammage) FOR months IN (' + @cols + ')
	) p
UNPIVOT
	( grammage FOR months IN (' + @cols + ')
	) u
'

execute(@query_gram)


SET @query_last_submit_date =
'
SELECT
	*
INTO cluster_movement_last_submit_date
FROM
	(
	SELECT
		b.id as member_id, a.months, a.created_at as last_submit_date
	FROM
		db_analytic_dancow.dbo.point_issued_dancow	a
	RIGHT JOIN
		db_analytic_dancow.dbo.dpr_submission_cluster_21_05 b ON a.member_id = b.id
	WHERE a.created_at >= b.created_at
	) x
PIVOT
	( MAX(last_submit_date) FOR months IN (' + @cols + ')
	) p
UNPIVOT
	( last_submit_date FOR months IN (' + @cols + ')
	) u
ORDER BY member_id, months
'

execute(@query_last_submit_date)

-- in development buat bikin field cut off tiap bulan
--SELECT 
--	*
--FROM
--(
--	SELECT
--	CAST(Date as date) cutoff.
--	CAST(YEAR(Date) as nvarchar) + '-' + RIGHT('0' + CAST(MONTH(Date) as nvarchar), 2)


SELECT a.*, isnull(b.grammage,0) as grammage, c.last_submit_date, ROW_NUMBER() OVER(PARTITION BY a.member_id ORDER BY a.months) as acc_age
INTO cluster_movement_temp
from cluster_movement_uc a
LEFT JOIN cluster_movement_gram b on a.member_id = b.member_id AND a.months = b.months
LEFT JOIN cluster_movement_last_submit_date c on a.member_id = c.member_id AND a.months = c.months
where a.months >= CAST(YEAR(created_at) as nvarchar) + '-' + RIGHT('0' + CAST(MONTH(created_at) as nvarchar), 2)


UPDATE cluster_movement_temp 
SET last_submit_date = (SELECT TOP 1 last_submit_date
              FROM cluster_movement_last_submit_date t              
              WHERE t.last_submit_date IS NOT NULL AND 
              b.months > t.months AND b.member_id = t.member_id
              ORDER BY t.months DESC)
FROM cluster_movement_temp b
WHERE b.last_submit_date IS NULL


DROP TABLE IF EXISTS cluster_movement_uc
DROP TABLE IF EXISTS cluster_movement_gram
DROP TABLE IF EXISTS cluster_movement_last_submit_date

SELECT *,
		SUM(unique_code) OVER(PARTITION BY member_id ORDER BY months ROWS 2 PRECEDING)
		as sum_uc_last3,
		SUM(grammage) OVER(PARTITION BY member_id ORDER BY months ROWS 2 PRECEDING)
		as sum_gram_last3,
		SUM(unique_code) OVER(PARTITION BY member_id ORDER BY months ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
		as sum_uc_all,
		AVG(grammage / 800 ) OVER(PARTITION BY member_id ORDER BY months ROWS 2 PRECEDING) avg_gram_last3
INTO cluster_movement_temp_2
FROM cluster_movement_temp

DROP TABLE IF EXISTS cluster_movement_temp 

SELECT *,
	  CASE
	  WHEN sum_uc_all = 0 then 'FANS'
	  WHEN sum_uc_all = 1 and sum_uc_last3 = 1 then '1st Entry'
	  WHEN sum_uc_all > 0 and sum_uc_last3 = 0 then 'Lapsed'
	  WHEN sum_uc_all > 0 and sum_uc_last3 > 0 and avg_gram_last3 < 3 then 'Upgrading'
	  WHEN sum_uc_all > 0 and sum_uc_last3 > 0 and avg_gram_last3 >= 3 then 'Loyal'
	  END as cluster
INTO cluster_movement
from cluster_movement_temp_2

DROP TABLE IF EXISTS cluster_movement_temp_2
DROP TABLE IF EXISTS point_issued_dancow


--select *
--from db_analytic_dancow.dbo.cluster_movement
--order by member_id, months