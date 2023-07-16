select event_name,
  (select value.string_value from unnest(event_params) where key = 'page_location') as event_page,
  count(distinct concat(event_timestamp, user_pseudo_id)) As event_count,
  count(distinct concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key = 'ga_session_id'))) event_sessions
  from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20*` 
where PARSE_DATE('%y%m%d', _TABLE_SUFFIX) between '2020-11-01' and '2021-01-31'
group by 1,2