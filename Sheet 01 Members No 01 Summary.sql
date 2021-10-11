declare @date datetime
set @date = '2021-06-01';


-- jangan lupa ganti table ya
with member as
(
 select a.id,
        a.created_at,
        a.status_member,
		a.flag_status_member,
		a.flag,
		case
		when a.channel IN ('crm', 'cc') then 'Call Center'
		when a.channel = 'sms' then 'SMS'
		when a.channel ='wa' then 'WA'
        when a.channel = 'api_bp' then 'BP Apps'
		else 'WEB' end channel,
		b.referrer as referrer,
		case
		when DATEDIFF(mm, a.child_dob, dateadd(m,-1,@date)) <= 35 then '1+'
		when DATEDIFF(mm, a.child_dob, dateadd(m,-1,@date)) <= 59 then '3+'
		when DATEDIFF(mm, a.child_dob, dateadd(m,-1,@date)) > 59 then '5+'
		else 'u' end child_age
		--case
		--when child_age <= 35 then '1+'
		--when child_age <= 59 then '3+'
		--when child_age > 59 then '5+'
		--else 'u' end child_age

 from db_analytic_dancow.dbo.dpr_submission_cluster_21_05 a
 left join db_analytic_dancow.dbo.snrv2_members b on a.id = b.id
 where a.created_at < @date
)
,
segment as
(
 select b.id member_id, b.cluster, b.flag, b.flag_status_member,  b.[1] + b.[2] + b.[3] sum_uc_last3, b.last_submit_date, b.created_at
 from db_analytic_dancow.dbo.dpr_submission_cluster_21_05 b 
 where b.status_member = 'valid' 
)
,
member_flag as
(
 select case
        when id is null then 0
		else 1 end flag_all_member,
		case
		when status_member = 'valid' then 1
		else 0 end flag_valid_member,
		case
		when status_member = 'valid' and flag = 'Others' and flag_status_member = 'valid' then 1
		else 0 end flag_valid_organic,
		case
		when status_member = 'valid' and flag <> 'Others' and flag_status_member = 'valid' then 1
		else 0 end flag_valid_bp,
		case
		when status_member = 'valid' and flag_status_member <> 'valid' then 1
		else 0 end flag_valid_dpc,
		case
		when status_member = 'valid' and YEAR(@date-1) > 2018 and created_at between DATEADD(mm, DATEDIFF(mm, 0, @date)-1, 0) and @date then 1
		when status_member = 'valid' and YEAR(@date-1) < 2019 and created_at < @date then 1
		else 0 end flag_new_register,
		case
		when status_member = 'valid' and YEAR(@date-1) > 2018 and created_at between DATEADD(mm, DATEDIFF(mm, 0, @date)-1, 0) and @date and flag = 'Others'
		and flag_status_member <> 'valid' then 1
		else 0 end flag_new_dpc,
		case
		when status_member = 'valid' and YEAR(@date-1) > 2018 and created_at between DATEADD(mm, DATEDIFF(mm, 0, @date)-1, 0) and @date and flag = 'Others'
		and flag_status_member = 'DPC GUM' then 1
		else 0 end flag_new_dpc_gum,
		case
		when status_member = 'valid' and YEAR(@date-1) > 2018 and created_at between DATEADD(mm, DATEDIFF(mm, 0, @date)-1, 0) and @date and flag = 'Others'
		and flag_status_member = 'DPC Fortigro' then 1
		else 0 end flag_new_dpc_fortigro,
		case
		when status_member = 'valid' and YEAR(@date-1) > 2018 and created_at between DATEADD(mm, DATEDIFF(mm, 0, @date)-1, 0) and @date and flag = 'Others'
		and flag_status_member = 'DPC Unknown' then 1
		else 0 end flag_new_dpc_unknown,
		case
		when status_member = 'valid' and flag_status_member = 'valid' and YEAR(@date-1) > 2018 and created_at between DATEADD(mm, DATEDIFF(mm, 0, @date)-1, 0) and @date and flag = 'Others' then 1
		when status_member = 'valid' and flag_status_member = 'valid' and YEAR(@date-1) < 2019 and created_at < @date and flag = 'Others' then 1
		else 0 end flag_new_organic,
		case
		when status_member = 'valid' and flag_status_member = 'valid' and YEAR(@date-1) > 2018 and created_at between DATEADD(mm, DATEDIFF(mm, 0, @date)-1, 0) and @date and flag = 'Others' 
		and channel = 'WEB' then 1 
		when status_member = 'valid' and flag_status_member = 'valid' and YEAR(@date-1) < 2019 and created_at < @date and flag = 'Others' 
		and channel = 'WEB' then 1
		else 0 end flag_new_organic_web,
		case
		when status_member = 'valid' and flag_status_member = 'valid' and YEAR(@date-1) > 2018 and created_at between DATEADD(mm, DATEDIFF(mm, 0, @date)-1, 0) and @date and flag = 'Others' 
		and channel = 'WEB' and referrer is null then 1 
		when status_member = 'valid' and flag_status_member = 'valid' and YEAR(@date-1) < 2019 and created_at < @date and flag = 'Others' 
		and channel = 'WEB' and referrer is null then 1
		else 0 end flag_new_organic_direct,
		case
		when status_member = 'valid' and flag_status_member = 'valid' and YEAR(@date-1) > 2018 and created_at between DATEADD(mm, DATEDIFF(mm, 0, @date)-1, 0) and @date and flag = 'Others' 
		and channel = 'WEB' and referrer is not null then 1 
		when status_member = 'valid' and flag_status_member = 'valid' and YEAR(@date-1) < 2019 and created_at < @date and flag = 'Others' 
		and channel = 'WEB' and referrer is not null then 1
		else 0 end flag_new_organic_MGM,
		case
		when status_member = 'valid' and YEAR(@date-1) > 2018 and created_at between DATEADD(mm, DATEDIFF(mm, 0, @date)-1, 0) and @date and flag = 'Others' 
		and channel = 'SMS' then 1 
		when status_member = 'valid' and YEAR(@date-1) < 2019 and created_at < @date and flag = 'Others' 
		and channel = 'SMS' then 1
		else 0 end flag_new_organic_SMS,
		case
		when status_member = 'valid' and YEAR(@date-1) > 2018 and created_at between DATEADD(mm, DATEDIFF(mm, 0, @date)-1, 0) and @date and flag = 'Others' 
		and channel = 'Call Center' then 1 
		when status_member = 'valid' and YEAR(@date-1) < 2019 and created_at < @date and flag = 'Others' 
		and channel = 'Call Center' then 1
		else 0 end flag_new_organic_CC,
		case
		when status_member = 'valid' and YEAR(@date-1) > 2018 and created_at between DATEADD(mm, DATEDIFF(mm, 0, @date)-1, 0) and @date and flag = 'Others' 
		and channel = 'WA' then 1 
		when status_member = 'valid' and YEAR(@date-1) < 2019 and created_at < @date and flag = 'Others' 
		and channel = 'WA' then 1 
		else 0 end flag_new_organic_WA,
		case
		when status_member = 'valid' and YEAR(@date-1) > 2018 and created_at between DATEADD(mm, DATEDIFF(mm, 0, @date)-1, 0) and @date and flag <> 'Others'
		then 1 
		when status_member = 'valid' and YEAR(@date-1) < 2019 and created_at < @date and flag <> 'Others'
		then 1
		else 0 end flag_new_BP,
		--kids age dpc
		case
		when status_member = 'valid' and flag_status_member <> 'valid' and child_age = '1+' then 1 else 0 end [flag_dpc_kids_age_1+],
		case
		when status_member = 'valid' and flag_status_member <> 'valid' and child_age = '3+' then 1 else 0 end [flag_dpc_kids_age_3+],
		case
		when status_member = 'valid' and flag_status_member <> 'valid' and child_age = '5+' then 1 else 0 end [flag_dpc_kids_age_5+],
		case
		when status_member = 'valid' and flag_status_member <> 'valid' and child_age = 'u' then 1 else 0 end [flag_dpc_kids_age_unknown],
		--kids age organic
		case
		when status_member = 'valid' and flag = 'Others' and flag_status_member = 'valid'  and child_age = '1+' then 1 else 0 end [flag_organic_kids_age_1+],
		case
		when status_member = 'valid' and flag = 'Others' and flag_status_member = 'valid' and child_age = '3+' then 1 else 0 end [flag_organic_kids_age_3+],
		case
		when status_member = 'valid' and flag = 'Others' and flag_status_member = 'valid' and child_age = '5+' then 1 else 0 end [flag_organic_kids_age_5+],
		case
		when status_member = 'valid' and flag = 'Others' and flag_status_member = 'valid' and child_age = 'u' then 1 else 0 end [flag_organic_kids_age_unknown],
		--kids age bp
		case
		when status_member = 'valid' and flag <> 'Others' and child_age = '1+' then 1 else 0 end [flag_bp_kids_age_1+],
		case
		when status_member = 'valid' and flag <> 'Others' and child_age = '3+' then 1 else 0 end [flag_bp_kids_age_3+],
		case
		when status_member = 'valid' and flag <> 'Others' and child_age = '5+' then 1 else 0 end [flag_bp_kids_age_5+],
		case
		when status_member = 'valid' and flag <> 'Others' and child_age = 'u' then 1 else 0 end [flag_bp_kids_age_unknown]
 from member
)
,
segment_flag as
(
 select case 
        when cluster = 'FANS' then 1 else 0 end flag_FS,
		case 
        when cluster = 'FANS' and flag_status_member <> 'valid' then 1 else 0 end flag_FS_DPC,
		case
		when cluster = 'FANS' and flag = 'Others'  and flag_status_member = 'valid'
		then 1 else 0 end flag_FS_Organic,
		case
		when cluster = 'FANS' and flag = 'Others' and flag_status_member = 'valid' and created_at >= DATEADD(mm, DATEDIFF(mm, 0, @date)-6, 0)
		then 1 else 0 end flag_FS_Organic_P6M,
		case
		when cluster = 'FANS' and flag = 'Others' and flag_status_member = 'valid' and created_at < DATEADD(mm, DATEDIFF(mm, 0, @date)-6, 0)
		then 1 else 0 end flag_FS_Organic_non_P6M,
		case
		when cluster = 'FANS' and flag <> 'Others' 
		then 1 else 0 end flag_FS_BP,
		case
		when cluster = 'FANS' and flag <> 'Others' and created_at >= DATEADD(mm, DATEDIFF(mm, 0, @date)-6, 0)
		then 1 else 0 end flag_FS_BP_P6M,
		case
		when cluster = 'FANS' and flag <> 'Others' and created_at < DATEADD(mm, DATEDIFF(mm, 0, @date)-6, 0)
		then 1 else 0 end flag_FS_BP_non_P6M,
		case 
        when cluster = 'Lapsed' then 1 else 0 end flag_Lapsed,
		case
		when cluster = 'Lapsed' and flag = 'Others' 
		then 1 else 0 end flag_Lapsed_Organic,
		case
		when cluster = 'Lapsed' and flag = 'Others' and last_submit_date between DATEADD(mm, DATEDIFF(mm, 0, @date)-6, 0)
		and DATEADD(mm, DATEDIFF(mm, 0, @date)-3, 0)
		then 1 else 0 end flag_Lapsed_Organic_P6M,
		case
		when cluster = 'Lapsed' and flag = 'Others' and last_submit_date < DATEADD(mm, DATEDIFF(mm, 0, @date)-6, 0)
		then 1 else 0 end flag_Lapsed_Organic_non_P6M,
		case
		when cluster = 'Lapsed' and flag <> 'Others' 
		then 1 else 0 end flag_Lapsed_BP,
		case
		when cluster = 'Lapsed' and flag <> 'Others' and last_submit_date between DATEADD(mm, DATEDIFF(mm, 0, @date)-6, 0)
		and DATEADD(mm, DATEDIFF(mm, 0, @date)-3, 0)
		then 1 else 0 end flag_Lapsed_BP_P6M,
		case
		when cluster = 'Lapsed' and flag <> 'Others' and last_submit_date < DATEADD(mm, DATEDIFF(mm, 0, @date)-6, 0)
		then 1 else 0 end flag_Lapsed_BP_non_P6M,
		case
		when sum_uc_last3 > 0 then 1 else 0 end flag_P3M,
		case
		when sum_uc_last3 > 0 and flag = 'Others' then 1 else 0 end flag_P3M_organic,
		case
		when sum_uc_last3 > 0 and flag = 'Others' and cluster = '1st Entry'
		then 1 else 0 end flag_P3M_organic_1st_Entry,
		case
		when sum_uc_last3 > 0 and flag = 'Others' and cluster = 'Upgrading'
		then 1 else 0 end flag_P3M_organic_Upgrading,
		case
		when sum_uc_last3 > 0 and flag = 'Others' and cluster = 'Loyal'
		then 1 else 0 end flag_P3M_organic_Loyal,
		case
		when sum_uc_last3 > 0 and flag <> 'Others' then 1 else 0 end flag_P3M_BP,
		case
		when sum_uc_last3 > 0 and flag <> 'Others' and cluster = '1st Entry'
		then 1 else 0 end flag_P3M_BP_1st_Entry,
		case
		when sum_uc_last3 > 0 and flag <> 'Others' and cluster = 'Upgrading'
		then 1 else 0 end flag_P3M_BP_Upgrading,
		case
		when sum_uc_last3 > 0 and flag <> 'Others' and cluster = 'Loyal'
		then 1 else 0 end flag_P3M_BP_Loyal
 from segment
)
,
dpr_table as
(
 select tbl.field, tbl.result
 from
  (
   select SUM(flag_all_member) [01. Number All Members],
          SUM(flag_valid_member) [02. Number Valid Members],
		  SUM(flag_valid_organic) [03. Number Valid Organic],
		  SUM(flag_valid_bp) [04. Number Valid BP],
		  SUM(flag_valid_dpc) [05. Number Valid DPC],
		  0 [06. Blank Space],
		  SUM(flag_new_register) [07. Number New Register],
		  SUM(flag_new_dpc) [08. Number New DPC],
		  SUM(flag_new_dpc_gum) [09. Number New DPC GUM],
		  SUM(flag_new_dpc_fortigro) [10. Number New DPC Fortigro],
		  SUM(flag_new_dpc_unknown) [11. Number New DPC Unknown],
		  SUM(flag_new_organic) [12. Number New Organic],
		  SUM(flag_new_organic_web) [13. Number New Organic WEB],
		  SUM(flag_new_organic_direct) [14. Number New Organic Direct],
		  SUM(flag_new_organic_MGM) [15. Number New Organic MGM],
		  SUM(flag_new_organic_SMS) [16. Number New Organic SMS],
		  SUM(flag_new_organic_CC) [17. Number New Organic CC],
		  SUM(flag_new_organic_WA) [18. Number New Organic WA],
		  SUM(flag_new_BP) [19. Number New BP],
		  0 [20. Blank Space],
		  SUM(flag_valid_member) [21. Number Member Kids Age],
		  SUM(flag_valid_dpc) [22. Number Valid DPC],
		  SUM([flag_dpc_kids_age_1+]) [23. Number DPC 1+],
		  SUM([flag_dpc_kids_age_3+]) [24. Number DPC 3+],
		  SUM([flag_dpc_kids_age_5+]) [25. Number DPC 5+],
		  SUM([flag_dpc_kids_age_unknown]) [26. Number DPC Unknown],
		  SUM(flag_valid_organic) [27. Number Organic],
		  SUM([flag_organic_kids_age_1+]) [28. Number Organic 1+],
		  SUM([flag_organic_kids_age_3+]) [29. Number Organic 3+],
		  SUM([flag_organic_kids_age_5+]) [30. Number Organic 5+],
		  SUM(flag_organic_kids_age_unknown) [31. Number Organic unknown],
		  SUM(flag_valid_bp) [32. Number BP],
		  SUM([flag_bp_kids_age_1+]) [33. Number BP 1+],
		  SUM([flag_bp_kids_age_3+]) [34. Number BP 3+],
		  SUM([flag_bp_kids_age_5+]) [35. Number BP 5+],
		  SUM(flag_bp_kids_age_unknown) [36. Number BP unknown],
		  0 [37. Blank Space]
   from member_flag) a
   UNPIVOT(result for field IN 
           (a.[01. Number All Members],
            a.[02. Number Valid Members],
			a.[03. Number Valid Organic],
			a.[04. Number Valid BP],
			a.[05. Number Valid DPC],
			a.[06. Blank Space],
			a.[07. Number New Register],
			a.[08. Number New DPC],
			a.[09. Number New DPC GUM],
			a.[10. Number New DPC Fortigro],
			a.[11. Number New DPC Unknown],
			a.[12. Number New Organic],
			a.[13. Number New Organic WEB],
			a.[14. Number New Organic Direct],
			a.[15. Number New Organic MGM],
			a.[16. Number New Organic SMS],
			a.[17. Number New Organic CC],
			a.[18. Number New Organic WA],
			a.[19. Number New BP],
			a.[20. Blank Space],
			a.[21. Number Member Kids Age],
			a.[22. Number Valid DPC],
			a.[23. Number DPC 1+],
			a.[24. Number DPC 3+],
			a.[25. Number DPC 5+],
			a.[26. Number DPC Unknown],
			a.[27. Number Organic],
			a.[28. Number Organic 1+],
			a.[29. Number Organic 3+],
			a.[30. Number Organic 5+],
			a.[31. Number Organic unknown],
			a.[32. Number BP],
			a.[33. Number BP 1+],
			a.[34. Number BP 3+],
			a.[35. Number BP 5+],
			a.[36. Number BP unknown],
			a.[37. Blank Space]
		   )) tbl
 UNION
 select tbl.field, tbl.result
 from
  (
   select SUM(flag_FS) [38. Number Fencesitter],
		  SUM(flag_FS_DPC) [39. Number DPC],
          SUM(flag_FS_organic) [40. Number Organic],
		  SUM(flag_FS_organic_P6M) [41. Number P6M],
		  SUM(flag_FS_organic_non_P6M) [42. Number non P6M],
		  SUM(flag_FS_BP) [43. Number BP],
		  SUM(flag_FS_BP_P6M) [44. Number P6M],
		  SUM(flag_FS_BP_non_P6M) [45. Number non P6M],
		  0 [46. Blank Space],
		  SUM(flag_Lapsed) [47. Number Lapsed],
		  SUM(flag_Lapsed_Organic) [48. Number Lapsed Organic],
		  SUM(flag_Lapsed_Organic_P6M) [49. Number P6M],
		  SUM(flag_Lapsed_Organic_non_P6M) [50. Number non P6M],
		  SUM(flag_Lapsed_BP) [51. Number Lapsed BP],
		  SUM(flag_Lapsed_BP_P6M) [52. Number P6M],
		  SUM(flag_Lapsed_BP_non_P6M) [53. Number non P6M],
		  0[54. Blank Space],
		  SUM(flag_P3M) [55. Number Active P3M],
		  SUM(flag_P3M_organic) [56. Number Organic],
		  SUM(flag_P3M_organic_1st_Entry) [57. Number 1st Entry],
		  SUM(flag_P3M_organic_Upgrading) [58. Number Upgrading],
		  SUM(flag_P3M_organic_Loyal) [59. Number Loyal],
		  SUM(flag_P3M_BP) [60. Number BP],
		  SUM(flag_P3M_BP_1st_Entry) [61. Number 1st Entry],
		  SUM(flag_P3M_BP_Upgrading) [62. Number Upgrading],
		  SUM(flag_P3M_BP_Loyal) [63. Number Loyal]
   from segment_flag)b
   UNPIVOT(result for field in 
           (b.[38. Number Fencesitter],
		    b.[39. Number DPC],
			b.[40. Number Organic],
			b.[41. Number P6M],
			b.[42. Number non P6M],
			b.[43. Number BP],
			b.[44. Number P6M],
			b.[45. Number non P6M],
			b.[46. Blank Space],
			b.[47. Number Lapsed],
			b.[48. Number Lapsed Organic],
			b.[49. Number P6M],
			b.[50. Number non P6M],
			b.[51. Number Lapsed BP],
			b.[52. Number P6M],
			b.[53. Number non P6M],
			b.[54. Blank Space],
			b.[55. Number Active P3M],
			b.[56. Number Organic],
			b.[57. Number 1st Entry],
			b.[58. Number Upgrading],
			b.[59. Number Loyal],
			b.[60. Number BP],
			b.[61. Number 1st Entry],
			b.[62. Number Upgrading],
			b.[63. Number Loyal]
		   ))tbl	
)

select field,
       case when result = 0 then '' else result end result
from dpr_table
order by field