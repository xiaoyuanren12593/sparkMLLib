INSERT INTO ods_policy_preserve_detail
SELECT
  UUID() AS id,
  a.id AS preserve_id,
  b.id AS policy_id,
  b.insurance_policy_no AS policy_code,
  b.user_code AS user_id,
  '4' AS preserve_status,
  a.inc_revise_no AS add_batch_code,
  a.inc_revise_premium AS add_premium,
  a.inc_revise_sum AS add_person_count,
  a.dec_revise_no AS del_batch_code,
  a.dec_revise_premium AS del_premium,
  a.dec_revise_sum AS del_person_count,
  (SELECT
    MIN(x.start_date)
  FROM
    odsdb.b_policy_preservation_subject_person_master X
  WHERE x.inc_dec_order_no = a.inc_dec_order_no) AS pre_start_date,
    MAX(x.end_date)
  FROM
  (SELECT
    odsdb.b_policy_preservation_subject_person_master X
  WHERE x.inc_dec_order_no = a.inc_dec_order_no) AS pre_end_date,
  a.create_time AS pre_create_time,
  a.update_time AS pre_update_time,
  a.preservation_type AS pre_type,
  c.id AS pre_insured_id,
  c.is_legal AS pre_is_legal,
  c.`name` AS insured_name,
  c.sex AS insured_gender,
  c.cert_no AS insured_cert_no,
  c.birthday AS insured_birthday,
  c.profession_code AS insured_profession,
  c.industry_code AS insured_industry,
  c.work_type AS insured_work_type,
  c.nation AS insured_nation,
  c.company_name AS insured_company_name,
  c.company_phone AS insured_company_phone,
  '1' AS is_chief_insurer,
  c.revise_status AS insured_changeType,
  c.start_date AS join_date,
  c.end_date AS left_date,
  CASE
    WHEN c.`status` = '1'
    THEN '0'
    ELSE '1'
  END AS insured_status,
  c.occu_category,
  c.create_time AS insured_create_time,
  c.update_time AS insured_update_time,
  d.id AS child_id,
  d.`name` AS child_name,
  d.sex AS child_gender,
  d.cert_type AS child_cert_type,
  d.cert_no AS child_cert_no,
  d.birthday AS child_birthday,
  d.nation AS child_nationality,
  d.join_date AS child_join_date,
  d.left_date AS child_left_date,
  NULL AS child_changeType,
  '' AS change_cause,
  d.create_time AS child_create_time,
  d.update_time AS child_update_time
FROM
  b_policy_preservation a
  LEFT JOIN b_policy b ON a.policy_no = b.policy_no
  LEFT JOIN b_policy_preservation_subject_person_master c
 ON a.policy_no = c.policy_no
    AND a.inc_dec_order_no = c.inc_dec_order_no
  LEFT JOIN b_policy_preservation_subject_person_slave d
    ON a.policy_no = d.policy_no
    AND a.inc_dec_order_no = d.inc_dec_order_no ;