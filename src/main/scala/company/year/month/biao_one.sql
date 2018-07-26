insert into ods_policy_preserve_detail
select
UUID() as id,
a.id as preserve_id,
ifnull(d.id, a.policy_id) as policy_id,
a.policy_code,
a.user_id,
a.`status` as preserve_status,
a.add_batch_code,
a.add_premium,
a.add_person_count,
a.del_batch_code,
a.del_premium,
a.del_person_count,
a.start_date as pre_start_date,
a.end_date as pre_end_date,
a.create_time as pre_create_time,
a.update_time as pre_update_time,
a.type as pre_type,
b.id as pre_insured_id,
b.is_legal as pre_is_legal,
b.`name` as insured_name,
b.gender as insured_gender,
b.cert_no as insured_cert_no,
b.birthday as insured_birthday,
b.profession as insured_profession,
b.industry as insured_industry,
b.work_type as insured_work_type,
b.nation as insured_nation,
b.company_name as insured_company_name,
b.company_phone as insured_company_phone,
b.is_chief_insurer,
b.changeType as insured_changeType,
b.join_date,
b.left_date,
b.`status` as insured_status,
b.occu_category,
b.create_time as insured_create_time,
b.update_time as insured_update_time,
c.id as child_id,
c.child_name,
c.child_gender,
c.child_cert_type,
c.child_cert_no,
c.child_birthday,
c.child_nationality,
c.child_join_date,e
c.child_left_date,
c.change_type as child_change_type,
c.change_cause,
c.create_time as child_create_time,
c.update_time as child_update_time
from plc_policy_preserve a
left join plc_policy_preserve_insured b on a.id=b.preserve_id
left join plc_policy_preserve_insured_child c on b.id=c.insured_id
left join b_policy d on a.policy_code=d.insurance_policy_no;