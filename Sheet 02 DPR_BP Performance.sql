/* REPLACE DATE & TABLE MONTH */

declare @date1 datetime
set @date1 = '2021-05-01' /* DATE */

SELECT months, 
	COUNT(DISTINCT ID) as total_member,
	COUNT(DISTINCT CASE WHEN rep1 >= 1 then id else null end) as repurchase1,
	COUNT(DISTINCT CASE WHEN rep2 >= 1 then id else null end) as repurchase2,
	COUNT(DISTINCT CASE WHEN rep3 >= 1 then id else null end) as repurchase3,
	COUNT(DISTINCT CASE WHEN ret1 >= 1 then id else null end) as retention1,
	COUNT(DISTINCT CASE WHEN ret2 >= 1 then id else null end) as retention2,
	COUNT(DISTINCT CASE WHEN ret3 >= 1 then id else null end) as retention3,
	COUNT(DISTINCT CASE WHEN retp1y >= 1 then id else null end) as retentionp1y,
	COUNT(DISTINCT CASE WHEN retp6m >= 1 then id else null end) as retentionp6m,
	COUNT(DISTINCT CASE WHEN retp3m >= 1 then id else null end) as retentionp3m,
	SUM(CASE WHEN rep1 >= 1 then gram_1 else null end) as total_gram_1,
	CASE WHEN datediff(m,months,@date1) <= 1 then SUM(CASE WHEN rep1 >= 1 then gram_1 else null end)/COUNT(DISTINCT CASE WHEN rep1 >= 1 then id else null end)/800/datediff(m,months,@date1) 
		 ELSE SUM(CASE WHEN rep1 >= 1 then gram_1 else null end)/COUNT(DISTINCT CASE WHEN rep1 >= 1 then id else null end)/800/2 end as avg_1,
	SUM(CASE WHEN rep2 >= 1 then gram_2 else null end) as total_gram_2,
	CASE WHEN datediff(m,months,@date1) <= 2 then SUM(CASE WHEN rep2 >= 1 then gram_2 else null end)/COUNT(DISTINCT CASE WHEN rep2 >= 1 then id else null end)/800/datediff(m,months,@date1) 
	     ELSE SUM(CASE WHEN rep2 >= 1 then gram_2 else null end)/COUNT(DISTINCT CASE WHEN rep2 >= 1 then id else null end)/800/3 end as avg_2,
	SUM(CASE WHEN rep3 >= 1 then gram_3 else null end) as total_gram_3,
	CASE WHEN datediff(m,months,@date1) <= 3 then SUM(CASE WHEN rep3 >= 1 then gram_3 else null end)/COUNT(DISTINCT CASE WHEN rep3 >= 1 then id else null end)/800/datediff(m,months,@date1)
	     ELSE SUM(CASE WHEN rep3 >= 1 then gram_3 else null end)/COUNT(DISTINCT CASE WHEN rep3 >= 1 then id else null end)/800/4 end as avg_3,
	SUM(CASE WHEN ret1 >= 1 then gram_m1 else null end) as total_gram_m1,
	SUM(CASE WHEN ret1 >= 1 then gram_m1 else null end)/COUNT(DISTINCT CASE WHEN ret1 >= 1 then id else null end)/800 as avg_m1,
	SUM(CASE WHEN ret2 >= 1 then gram_m2 else null end) as total_gram_m2,
	SUM(CASE WHEN ret2 >= 1 then gram_m2 else null end)/COUNT(DISTINCT CASE WHEN ret2 >= 1 then id else null end)/800 as avg_m2,
	SUM(CASE WHEN ret3 >= 1 then gram_m3 else null end) as total_gram_m3,
	SUM(CASE WHEN ret3 >= 1 then gram_m3 else null end)/COUNT(DISTINCT CASE WHEN ret3 >= 1 then id else null end)/800 as avg_m3,
	SUM(CASE WHEN retp1y >= 1 then gram_p1y else null end) as total_gram_p1y,
	CASE WHEN datediff(m,months,@date1) < 12 then SUM(CASE WHEN retp1y >= 1 then gram_p1y else null end)/COUNT(DISTINCT CASE WHEN retp1y >= 1 then id else null end)/800/datediff(m,months,@date1)
		 ELSE SUM(CASE WHEN retp1y >= 1 then gram_p1y else null end)/COUNT(DISTINCT CASE WHEN retp1y >= 1 then id else null end)/800/12 end as avg_p1y,
	SUM(CASE WHEN retp6m >= 1 then gram_p6m else null end) as total_gram_p6m,
	CASE WHEN datediff(m,months,@date1) < 6 then SUM(CASE WHEN retp6m >= 1 then gram_p6m else null end)/COUNT(DISTINCT CASE WHEN retp6m >= 1 then id else null end)/800/datediff(m,months,@date1)
		 ELSE SUM(CASE WHEN retp6m >= 1 then gram_p6m else null end)/COUNT(DISTINCT CASE WHEN retp6m >= 1 then id else null end)/800/6 end as avg_p6m,
	SUM(CASE WHEN retp3m >= 1 then gram_p3m else null end) as total_gram_p3m,
	CASE WHEN datediff(m,months,@date1) < 3 then SUM(CASE WHEN retp3m >= 1 then gram_p3m else null end)/COUNT(DISTINCT CASE WHEN retp3m >= 1 then id else null end)/800/datediff(m,months,@date1)
		 ELSE SUM(CASE WHEN retp3m >= 1 then gram_p3m else null end)/COUNT(DISTINCT CASE WHEN retp3m >= 1 then id else null end)/800/3 end as avg_p3m,
	COUNT(DISTINCT CASE WHEN ret1 >= 1 and ret2>=1 and ret3>=1 then id else null end) as stable_all
FROM
(
SELECT 
	a.id,
	COUNT(case when b.created_at between DATEADD(mm, DATEDIFF(mm, 0, a.created_at), 0) and DATEADD(mm, DATEDIFF(mm, 0, a.created_at)+2, 0) and b.created_at > e.first_submit_date then b.unique_code else null end) rep1,
	COUNT(case when b.created_at between DATEADD(mm, DATEDIFF(mm, 0, a.created_at), 0) and DATEADD(mm, DATEDIFF(mm, 0, a.created_at)+3, 0) and b.created_at > e.first_submit_date then b.unique_code else null end) rep2,
	COUNT(case when b.created_at between DATEADD(mm, DATEDIFF(mm, 0, a.created_at), 0) and DATEADD(mm, DATEDIFF(mm, 0, a.created_at)+4, 0) and b.created_at > e.first_submit_date  then b.unique_code else null end) rep3,

	COUNT(case when b.created_at between DATEADD(mm, DATEDIFF(mm, 0, a.created_at)+1, 0) and DATEADD(mm, DATEDIFF(mm, 0, a.created_at)+2, 0) and b.created_at > e.first_submit_date then b.unique_code else null end) ret1,
	COUNT(case when b.created_at between DATEADD(mm, DATEDIFF(mm, 0, a.created_at)+2, 0) and DATEADD(mm, DATEDIFF(mm, 0, a.created_at)+3, 0) and b.created_at > e.first_submit_date then b.unique_code else null end) ret2,
	COUNT(case when b.created_at between DATEADD(mm, DATEDIFF(mm, 0, a.created_at)+3, 0) and DATEADD(mm, DATEDIFF(mm, 0, a.created_at)+4, 0) and b.created_at > e.first_submit_date  then b.unique_code else null end) ret3,
	COUNT(case when b.created_at between DATEADD(mm, DATEDIFF(mm, 0, @date1)-3, 0) and @date1 and b.created_at > e.first_submit_date then b.unique_code else null end) retp3m,
	COUNT(case when b.created_at between DATEADD(mm, DATEDIFF(mm, 0, @date1)-6, 0) and @date1 and b.created_at > e.first_submit_date then b.unique_code else null end) retp6m,
	COUNT(case when b.created_at between DATEADD(mm, DATEDIFF(mm, 0, @date1)-12, 0) and @date1 and b.created_at > e.first_submit_date then b.unique_code else null end) retp1y,

	COUNT(case when b.created_at between DATEADD(mm, DATEDIFF(mm, 0, a.created_at), 0) and @date1 then b.unique_code else null end) total_uc,

	SUM(case when b.created_at between DATEADD(mm, DATEDIFF(mm, 0, a.created_at), 0) and DATEADD(mm, DATEDIFF(mm, 0, a.created_at)+2, 0) and b.created_at > e.first_submit_date then b.grammage else null end) gram_1,
	SUM(case when b.created_at between DATEADD(mm, DATEDIFF(mm, 0, a.created_at), 0) and DATEADD(mm, DATEDIFF(mm, 0, a.created_at)+3, 0) and b.created_at > e.first_submit_date then b.grammage else null end) gram_2,
	SUM(case when b.created_at between DATEADD(mm, DATEDIFF(mm, 0, a.created_at), 0) and DATEADD(mm, DATEDIFF(mm, 0, a.created_at)+4, 0) and b.created_at > e.first_submit_date then b.grammage else null end) gram_3,

	SUM(case when b.created_at between DATEADD(mm, DATEDIFF(mm, 0, a.created_at)+1, 0) and DATEADD(mm, DATEDIFF(mm, 0, a.created_at)+2, 0) and b.created_at > e.first_submit_date then b.grammage else null end) gram_m1,
	SUM(case when b.created_at between DATEADD(mm, DATEDIFF(mm, 0, a.created_at)+2, 0) and DATEADD(mm, DATEDIFF(mm, 0, a.created_at)+3, 0) and b.created_at > e.first_submit_date then b.grammage else null end) gram_m2,
	SUM(case when b.created_at between DATEADD(mm, DATEDIFF(mm, 0, a.created_at)+3, 0) and DATEADD(mm, DATEDIFF(mm, 0, a.created_at)+4, 0) and b.created_at > e.first_submit_date then b.grammage else null end) gram_m3,

	SUM(case when b.created_at between DATEADD(mm, DATEDIFF(mm, 0, @date1)-3, 0) and @date1 and b.created_at > e.first_submit_date then b.grammage else null end) gram_p3m,
	SUM(case when b.created_at between DATEADD(mm, DATEDIFF(mm, 0, @date1)-6, 0) and @date1 and b.created_at > e.first_submit_date then b.grammage else null end) gram_p6m,
	SUM(case when b.created_at between DATEADD(mm, DATEDIFF(mm, 0, @date1)-12, 0) and @date1 and b.created_at > e.first_submit_date then b.grammage else null end) gram_p1y,
	SUM(case when b.created_at between DATEADD(mm, DATEDIFF(mm, 0, a.created_at), 0) and @date1 and b.created_at > e.first_submit_date then b.grammage else null end) total_gram,

	DATEADD(mm, DATEDIFF(mm, 0, a.created_at), 0) as months
FROM db_analytic_dancow.dbo.dpr_submission_cluster_21_04 a
LEFT JOIN (select * from db_analytic_dancow.dbo.dpr_point_issued where brand = 'dancow' and created_at < @date1) b on a.id = b.member_id
LEFT JOIN 
		(select distinct member_id, first_value(product)over(partition by member_id order by created_at) as first_submit,
			first_value(created_at)over(partition by member_id order by created_at) as first_submit_date
			from db_analytic_dancow.dbo.dpr_point_issued where brand = 'dancow' and created_at < @date1) e on a.id = e.member_id
WHERE status_member = 'valid' and flag <> 'Others' 
GROUP BY a.id, DATEADD(mm, DATEDIFF(mm, 0, a.created_at), 0)
) a 
group by months
order by months