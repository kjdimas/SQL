declare @date datetime
set @date = '2021-10-01';
with 
dpr_raw as (
			select	a.id, a.member_code, b.email, a.hp_1, b.city as kota, a.child_id , a.last_submit_date , 
					case
					when a.channel in ('crm','cc') then 'Call Center'
					when a.channel = 'sms' then 'SMS'
					when a.channel in ('wa','whatsapp','bp_whatsapp')  then 'WA'
					else 'WEB' end as channel, a.gender
			from db_analytic_dancow.dbo.dpr_submission_cluster_21_09 a
			left join db_analytic_dancow.dbo.snrv2_members b on a.id = b.id
			--left join 
			--(
			--select *
			--from
			--(
			--select *, ROW_NUMBER()OVER(PARTITION BY member_id order by id) as rn
			--from
			--db_analytic_dancow.dbo.member_detail
			--) X where rn = 1
			--)
			--db_analytic_dancow.dbo.member_detail c on a.id = c.member_id
			--where a.status_member = 'valid'
			)
,
dpr_member as(
			select id,member_code,email,hp_1,kota,child_id,last_submit_date,gender,channel
			from 
				( select *,ROW_NUMBER() over(partition by id order by id) as rn
				from dpr_raw ) a
			where rn = 1
			)
,
dpr_child as (
			select a.* 
			from db_analytic_dancow.dbo.snrv2_children a
			inner join dpr_member b on a.member_id = b.id
			 )
,
dpr_member_flag as(
			select 
				case 
					when id is null then 0
					else 1
					end as flag_username,
				case 
					when email is null then 0
					else 1
					end as flag_email,
				case 
					when hp_1 is null then 0
					else 1
					end as flag_hp,
				case 
					when kota is null then 0
					else 1
					end as flag_address,
				case 
					when gender = 'm' then 1
					else 0
					end as flag_male_gender,
				case 
					when gender = 'f' then 1
					else 0
					end as flag_female_gender,
				case 
					when gender is null then 1
					else 0
					end as flag_unknown_gender,
				case 
					when channel = 'Call Center' then 1
					else 0
					end as flag_channel_call,
				case 
					when channel = 'SMS' then 1
					else 0
					end as flag_channel_sms,
				case 
					when channel = 'WEB' then 1
					else 0
					end as flag_channel_web,
				case 
					when channel = 'WA' then 1
					else 0
					end as flag_channel_wa,
				case
					when child_id is not null then 1
					else 0
					end as flag_member_child,
				case
					when last_submit_date between dateadd(month,-24,@date) and @date then 1
					else 0
					end as flag_active_24mo

				from dpr_member
				)
,
dpr_child_flag as(
			select
				case
					when gender = 'f' then 1
					else 0
					end as flag_female_child_gender,
				case
					when gender = 'm' then 1
					else 0
					end as flag_male_child_gender,
				case
					when gender is null then 1
					else 0
					end as flag_unknown_child_gender
			from dpr_child
			)
,
dpr_table as(
			select tblPivot.Property, tblPivot.Value
			FROM
				(select 
					SUM(flag_username) as [1. Number of consumers], 
					SUM(flag_username) as [2. Number of consumers with username], 
					SUM(flag_email) as [3. Number of consumers with email], 
					SUM(flag_hp) as [4. Number of consumers with hp], 
					SUM(flag_address) as [5. Number of consumers with address], 
					SUM(flag_male_gender) as [6. Number of male consumers], 
					SUM(flag_female_gender) as [7. Number of female consumers], 
					SUM(flag_unknown_gender) as [8. Number of undefined gender consumers], 
					SUM(flag_channel_sms) as [9. Number of SMS Registration],
					SUM(flag_channel_web) as [10. Number of WEB Registration],
					SUM(flag_channel_Call) as [11. Number of Call Center Registration],
					SUM(flag_channel_wa) as [12. Number of Whatsapp Registration],
					SUM(flag_member_child) as [13. Number of child name], 
					SUM(flag_member_child) as [14. Number of child dob],
					SUM(flag_active_24mo) as [18. Number of consumers active 24mo]
				from dpr_member_flag) a
				UNPIVOT (Value For Property In ( a.[1. Number of consumers],
												 a.[2. Number of consumers with username],
												 a.[3. Number of consumers with email],
												 a.[4. Number of consumers with hp],
												 a.[5. Number of consumers with address],
												 a.[6. Number of male consumers],
												 a.[7. Number of female consumers], 
												 a.[8. Number of undefined gender consumers],
												 a.[9. Number of SMS Registration],
												 a.[10. Number of WEB Registration],
												 a.[11. Number of Call Center Registration],
												 a.[12. Number of Whatsapp Registration],
												 a.[13. Number of child name],
												 a.[14. Number of child dob],
												 a.[18. Number of consumers active 24mo]
												 )) as tblPivot
						
						UNION

				select tblPivot.Property, tblPivot.Value 
				from
					(select
						SUM(flag_male_child_gender) as [15. Number of male child],
						SUM(flag_female_child_gender) as [16. Number of female child],
						SUM(flag_unknown_child_gender) as [17. Number of unknown child]
					from dpr_child_flag) b
					UNPIVOT (Value For Property In ( b.[15. Number of male child],
													 b.[16. Number of female child],
													 b.[17. Number of unknown child]
													 )) as tblPivot
					
					)
,
snr_raw as (
			select	a.id, a.member_code, b.email, a.hp_1, b.city, a.child_id , a.last_submit_date ,
					a.channel, a.gender
			from db_analytic_lactogrow.dbo.snr_submission_cluster_21_09 a
			left join db_analytic_lactogrow.dbo.snrv2_members b on a.id = b.id
			--where a.status_member = 'valid'
			)
,
snr_member as(
			select id,member_code,email,hp_1,city,child_id,last_submit_date,gender,channel 
			from 
				( select *,ROW_NUMBER() over(partition by id order by id) as rn
				from snr_raw ) a
			where rn = 1)
,
snr_child as (
			select a.* 
			from db_analytic_lactogrow.dbo.snrv2_children a
			inner join snr_member b on a.member_id = b.id
			 )
,
snr_member_flag as(
			select 
				case 
					when member_code is null then 0
					else 1
					end as flag_username,
				case 
					when email is null then 0
					else 1
					end as flag_email,
				case 
					when hp_1 is null then 0
					else 1
					end as flag_hp,
				case 
					when city is null then 0
					else 1
					end as flag_address,
				case 
					when gender = 'm' then 1
					else 0
					end as flag_male_gender,
				case 
					when gender = 'f' then 1
					else 0
					end as flag_female_gender,
				case 
					when gender is null or gender = 'u' then 1
					else 0
					end as flag_unknown_gender,
				case 
					when channel = 'cc' then 1
					else 0
					end as flag_channel_call,
				case 
					when channel = 'sms' then 1
					else 0
					end as flag_channel_sms,
				case 
					when channel in ('web','api_bp') or channel is null  then 1
					else 0
					end as flag_channel_web,
				case 
					when channel = 'wa' then 1
					else 0
					end as flag_channel_wa,
				case
					when child_id is not null then 1
					else 0
					end as flag_member_child,
				case
					when last_submit_date between dateadd(month,-24,@date) and @date then 1
					else 0
					end as flag_active_24mo

				from snr_member
				)
,
snr_child_flag as(
			select
				case
					when gender = 'f' then 1
					else 0
					end as flag_female_child_gender,
				case
					when gender = 'm' then 1
					else 0
					end as flag_male_child_gender,
				case
					when gender is null or gender not in ('f','m') then 1
					else 0
					end as flag_unknown_child_gender
			from snr_child
			)
,
snr_table as(
			select tblPivot.Property, tblPivot.Value
			FROM
				(select 
					SUM(flag_username) as [1. Number of consumers], 
					SUM(flag_username) as [2. Number of consumers with username], 
					SUM(flag_email) as [3. Number of consumers with email], 
					SUM(flag_hp) as [4. Number of consumers with hp], 
					SUM(flag_address) as [5. Number of consumers with address], 
					SUM(flag_male_gender) as [6. Number of male consumers], 
					SUM(flag_female_gender) as [7. Number of female consumers], 
					SUM(flag_unknown_gender) as [8. Number of undefined gender consumers], 
					SUM(flag_channel_sms) as [9. Number of SMS Registration],
					SUM(flag_channel_web) as [10. Number of WEB Registration],
					SUM(flag_channel_Call) as [11. Number of Call Center Registration], 
					SUM(flag_channel_wa) as [12. Number of Whatsapp Registration], 
					SUM(flag_member_child) as [13. Number of child name], 
					SUM(flag_member_child) as [14. Number of child dob],
					SUM(flag_active_24mo) as [18. Number of consumers active 24mo]
				from snr_member_flag) a
				UNPIVOT (Value For Property In ( a.[1. Number of consumers],
												 a.[2. Number of consumers with username],
												 a.[3. Number of consumers with email],
												 a.[4. Number of consumers with hp],
												 a.[5. Number of consumers with address],
												 a.[6. Number of male consumers],
												 a.[7. Number of female consumers], 
												 a.[8. Number of undefined gender consumers],
												 a.[9. Number of SMS Registration],
												 a.[10. Number of WEB Registration],
												 a.[11. Number of Call Center Registration],
												 a.[12. Number of Whatsapp Registration],
												 a.[13. Number of child name],
												 a.[14. Number of child dob],
												 a.[18. Number of consumers active 24mo]
												 )) as tblPivot
						
						UNION

				select tblPivot.Property, tblPivot.Value 
				from
					(select
						SUM(flag_male_child_gender) as [15. Number of male child],
						SUM(flag_female_child_gender) as [16. Number of female child],
						SUM(flag_unknown_child_gender) as [17. Number of unknown child]
					from snr_child_flag) b
					UNPIVOT (Value For Property In ( b.[15. Number of male child],
													 b.[16. Number of female child],
													 b.[17. Number of unknown child]
													 )) as tblPivot
					
					)
,
hasil_dpr as(
				SELECT 
					CAST(SUBSTRING([Property], 1, CHARINDEX('.', [Property])-1) AS INT) AS no,
					SUBSTRING([Property], CHARINDEX('.', [Property])+2, LEN([Property])) AS 'Property',
					Value
				FROM dpr_table
				 )
,
hasil_snr as(
				SELECT 
					CAST(SUBSTRING([Property], 1, CHARINDEX('.', [Property])-1) AS INT) AS no,
					SUBSTRING([Property], CHARINDEX('.', [Property])+2, LEN([Property])) AS 'Property',
					Value
				FROM snr_table
				 )
SELECT 
		a.[Property],
		a.[Value] AS 'Value for DPR',
		b.[Value] AS 'Value for SNR'
FROM hasil_dpr a
LEFT JOIN hasil_snr b ON a.no = b.no
ORDER BY a.no ASC

