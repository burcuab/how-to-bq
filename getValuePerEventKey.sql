select  event_name,
        key,
        COALESCE(value.string_value, cast(value.int_value as string), cast(value.double_value as string),cast(value.float_value as string)) as value
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20*`, unnest(event_params)
where PARSE_DATE('%y%m%d', _TABLE_SUFFIX) between '2020-11-01' and '2021-01-31'
and event_name='purchase'