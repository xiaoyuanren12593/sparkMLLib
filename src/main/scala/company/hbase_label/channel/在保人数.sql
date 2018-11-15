select trim(if(trim(x.channel_name)='直客', x.ent_name, x.channel_name)) as holder_company,
x.salesman as sales_name,
x.channel_name as channel_name,
d.name as province_name,
ifnull(sum(aa.insured_cnt),0) as curr_cnt
from ods_policy_detail a
left join ods_ent_guzhu_salesman x on trim(a.holder_company)=trim(x.ent_name)
left join dwdb.dim_product b on a.insure_code=b.product_code
#本周在保
left join (
select a.policy_id, sum(a.curr_insured) as insured_cnt from ods_policy_curr_insured a 
where a.day_id='20181106'
group by a.policy_id
) aa on aa.policy_id=a.policy_id
left join ods_policy_province c on a.policy_id=c.policy_id
left join dict_cant d on c.office_province=d.`code`
where b.product_type_2='蓝领外包' and b.dim_1 in ('外包雇主','骑士保','大货车') and a.policy_status in ('0','1')
and x.channel_name like '%招才通%'
group by trim(if(trim(x.channel_name)='直客', x.ent_name, x.channel_name))
having curr_cnt>=1000
order by curr_cnt desc;