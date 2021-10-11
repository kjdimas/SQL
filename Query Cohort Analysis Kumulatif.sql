with cte as(
select member_id, min(created_at) as first_submit from dpc.dbo.point_issued_dancow 
group by member_id)

select distinct * from (
select 
	distinct 
	dateadd(m,datediff(m,0,a.created_at),0) as months,
	case when b.created_at between dateadd(m,datediff(m,0,a.created_at),0) and dateadd(m,4,dateadd(m,datediff(m,0,a.created_at),0)) then a.id
	end as active, flag
from dpc.dbo.dpr_submission_cluster_20_06 a 
left join dpc.dbo.point_issued_dancow b on a.id = b.member_id
left join cte c on a.id = c.member_id 
where b.created_at > c.first_submit  and a.status_member = 'valid'
)x
pivot(count(active) for flag in ([Others],[BP DANCOW]))pvt
order by months