UPDATE `burcuproject.dataset2.ga_sessions_20170801`
SET hits = ARRAY(
  SELECT AS STRUCT * REPLACE((
    SELECT AS STRUCT * REPLACE(
      CASE WHEN eventCategory='Enhanced Ecommerce' and eventAction='Quickview Click' and not REGEXP_CONTAINS(eventLabel, '.*Phone.*') then 'PII_REMOVED' else eventLabel end AS eventLabel) FROM UNNEST([eventInfo])) AS eventInfo) FROM UNNEST(hits))
WHERE true