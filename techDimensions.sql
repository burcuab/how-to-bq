select 
  device.category as device_category,
  device.operating_system as operating_system,
  device.operating_system_version as operating_system_version,
  device.web_info.browser as browser,
  device.web_info.browser_version as browser_version,
  count(distinct user_pseudo_id) as users,
  count(distinct concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key = 'ga_session_id'))) sessions,
  count(distinct case when event_name = 'purchase' then concat(event_timestamp, user_pseudo_id) else null end) As transactions
  from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20*` 
where PARSE_DATE('%y%m%d', _TABLE_SUFFIX) between '2020-11-01' and '2021-01-31'
group by 1,2,3,4,5,6,7