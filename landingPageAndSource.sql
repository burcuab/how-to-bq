select 
  case when (select value.int_value from unnest(event_params) where event_name = 'page_view' and key = 'entrances') = 1 then (select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'page_location') end as landing_page,
  traffic_source.source as source,
  traffic_source.medium as medium,
  traffic_source.name as campaign,
  count(distinct concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key = 'ga_session_id'))) sessions,
  count(distinct case when (select value.string_value from unnest(event_params) where key = 'session_engaged') = '1' then concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key = 'ga_session_id')) end) as engaged_sessions,
  count(distinct concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key = 'ga_session_id'))) - count(distinct case when (select value.string_value from unnest(event_params) where key = 'session_engaged') = '1' then concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key = 'ga_session_id')) end) as bounces,
  (count(distinct concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key = 'ga_session_id'))) - count(distinct case when (select value.string_value from unnest(event_params) where key = 'session_engaged') = '1' then concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key = 'ga_session_id')) end))/ifnull(count(distinct concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key = 'ga_session_id'))),0) as bounce_rate,
  count(distinct case when event_name = 'purchase' then concat(event_timestamp, user_pseudo_id) else null end) As transactions
  from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20*` 
where PARSE_DATE('%y%m%d', _TABLE_SUFFIX) between '2020-11-01' and '2021-01-31'
group by 1,2,3,4