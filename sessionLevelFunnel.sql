with hits as (
  SELECT 
    distinct
    event_date,
    event_timestamp,
    concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key = 'ga_session_id')) as sessionid,
    user_pseudo_id as userid,
    event_name as step
  FROM 
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131` 
  where 
    event_name in ('view_item_list','view_item','add_to_cart','remove_from_cart','view_promotion','select_promotion','begin_checkout','add_shipping_info','add_payment_info','purchase')
),

sessionFunnel as (
  SELECT    
    sessionid, userid, event_date, STRING_AGG(step ORDER BY event_timestamp) funnelStr
  FROM
    hits
  GROUP BY
    all
)

select 
  event_date,
  count(sessionid) as sessions_all,
  count(case when regexp_contains(funnelStr, '.*view_item.*') then sessionid else null end) as sessions_with_item_view,
  count(case when regexp_contains(funnelStr, '.*view_item.*add_to_cart.*') then sessionid else null end) as sessions_with_itemview_addtocart,
  count(case when regexp_contains(funnelStr, '.*view_item.*add_to_cart.*begin_checkout.*') then sessionid else null end) as sessions_with_itemview_addtocart_checkout,
  count(case when regexp_contains(funnelStr, '.*view_item.*add_to_cart.*begin_checkout.*purchase.*') then sessionid else null end) as sessions_with_itemview_addtocart_checkout_purchase,
  count(case when regexp_contains(funnelStr, '.*begin_checkout.*') then sessionid else null end) as sessions_with_checkoutstep1,
  count(case when regexp_contains(funnelStr, '.*begin_checkout.*add_shipping_info.*') then sessionid else null end) as sessions_with_checkoutstep1_2,
  count(case when regexp_contains(funnelStr, '.*begin_checkout.*add_shipping_info.*add_payment_info.*') then sessionid else null end) as sessions_with_checkoutstep1_2_3,
  count(case when regexp_contains(funnelStr, '.*begin_checkout.*add_shipping_info.*add_payment_info.*purchase.*') then sessionid else null end) as sessions_with_checkoutstep1_2_3_purchase
from 
  sessionFunnel
group by 
  all;