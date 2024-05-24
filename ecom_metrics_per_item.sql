SELECT
  items.item_id,
  items.item_name,
  items.item_category,
  sum(ifnull(case when event_name='view_item_list' then items.quantity else 0 end,0)) AS item_list_views,
  sum(ifnull(case when event_name='select_item' then items.quantity else 0 end,0)) AS item_list_clicks,
  sum(ifnull(case when event_name='view_item' then items.quantity else 0 end,0)) AS item_views,
  sum(ifnull(case when event_name='add_to_cart' then items.quantity else 0 end,0)) AS items_added_to_cart,
  sum(ifnull(case when event_name='remove_from_cart' then items.quantity else 0 end,0)) AS items_removed_from_cart,
  sum(ifnull(case when event_name='begin_checkout' then items.quantity else 0 end,0)) AS items_checked_out,
  sum(ifnull(case when event_name='purchase' then items.quantity else 0 end,0)) AS items_purchased,
  sum(ifnull(case when event_name='purchase' then items.item_revenue else 0 end,0)) AS item_revenue
FROM   `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20*`, UNNEST(items) AS items
WHERE
  PARSE_DATE('%y%m%d', _TABLE_SUFFIX) BETWEEN '2020-11-01' AND '2021-01-31'
  AND event_name in ('view_item','view_item_list','select_item','add_to_cart','remove_from_cart','begin_checkout','purchase')
GROUP BY
  all