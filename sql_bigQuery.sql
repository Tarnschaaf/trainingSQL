SELECT
  cs.segments_date as date,
  CAST(c.campaign_id AS STRING) AS campaign_id,
  c.campaign_name,
  c.campaign_advertising_channel_type as campaign_type_ads,



      CASE
    WHEN REGEXP_CONTAINS(c.campaign_name, r'^c:([A-Z]{2})') THEN REGEXP_EXTRACT(c.campaign_name, r'^c:([A-Z]{2})')
    WHEN REGEXP_CONTAINS(c.campaign_name, r'^#LUP\b.*') THEN REGEXP_EXTRACT(c.campaign_name, r'^#LUP\b\s+#([A-Z]{2})')
    WHEN REGEXP_CONTAINS(c.campaign_name, r'^[A-Z]{2}\s.*') THEN REGEXP_EXTRACT(c.campaign_name, r'^([A-Z]{2})\s')
    
    -- Prüfen auf Kombinationen von Ländercodes mit Trennzeichen in campaign_name
    WHEN REGEXP_CONTAINS(c.campaign_name, r'#([A-Z]{2})[-_+][A-Z]{2}') THEN "Other"
    
    -- Prüfen auf explizite Ländercodes in campaign_name
    WHEN REGEXP_CONTAINS(c.campaign_name, r'#(?:AL|AD|AT|BY|BE|BA|BG|HR|CY|CZ|DK|EE|FI|FR|DE|GR|HU|IS|IE|IT|LV|LT|LU|MT|MD|MC|ME|NL|MK|NO|PL|PT|RO|RU|SM|RS|SK|SI|ES|SE|CH|UA|GB|VA|TR)\s') THEN REGEXP_EXTRACT(c.campaign_name, r'#(AL|AD|AT|BY|BE|BA|BG|HR|CY|CZ|DK|EE|FI|FR|DE|GR|HU|IS|IE|IT|LV|LT|LU|MT|MD|MC|ME|NL|MK|NO|PL|PT|RO|RU|SM|RS|SK|SI|ES|SE|CH|UA|GB|VA|TR)\s')
     WHEN REGEXP_CONTAINS(c.campaign_name, r':(?:AL|AD|AT|BY|BE|BA|BG|HR|CY|CZ|DK|EE|FI|FR|DE|GR|HU|IS|IE|IT|LV|LT|LU|MT|MD|MC|ME|NL|MK|NO|PL|PT|RO|RU|SM|RS|SK|SI|ES|SE|CH|UA|GB|VA|TR)\s') THEN REGEXP_EXTRACT(c.campaign_name, r':(AL|AD|AT|BY|BE|BA|BG|HR|CY|CZ|DK|EE|FI|FR|DE|GR|HU|IS|IE|IT|LV|LT|LU|MT|MD|MC|ME|NL|MK|NO|PL|PT|RO|RU|SM|RS|SK|SI|ES|SE|CH|UA|GB|VA|TR)\s')
    
    -- Prüfen auf Ländercode gefolgt von Sprache im Format "_fc-XX-xx" in campaign_name
    WHEN REGEXP_CONTAINS(c.campaign_name, r'_fc-([A-Z]{2})-[a-z]{2}') THEN REGEXP_EXTRACT(c.campaign_name, r'_fc-([A-Z]{2})-[a-z]{2}')



    ELSE "other"
  END AS country,

  -- metrics
  SUM(cs.metrics_impressions) AS impressions,
  SUM(cs.metrics_clicks) AS clicks,
  SUM(CAST(cs.metrics_conversions AS INT64)) AS conversions,
  SUM(CAST(cs.metrics_conversions AS INT64)) AS job_conversions,
  (SUM(cs.metrics_cost_micros) / 1000000) AS cost,

  -- join parameters
  COALESCE(ls.utm_source, ls_bridge.utm_source) AS utm_source,
  COALESCE(ls.utm_medium, ls_bridge.utm_medium) AS utm_medium,
  COALESCE(ls.utm_campaign, ls_bridge.utm_campaign) AS utm_campaign,
  COALESCE(ls.utm_content, ls_bridge.utm_content) AS utm_content,

  -- placeholder
  "none" as data_source_id, -- funnel
  CONCAT('Gads MCC - ',ac.customer_descriptive_name) as data_source_name, ## ADDING 02-07-2024 !

 # 'raw_gads_mcc_finstral' as data_source_name, -- funnel #
  'Google Ads' as data_source_type_name, -- funnel
  'Google' as platform, -- funnel
  'Google' as traffic_source,
  'none' as media_type, -- funnel
  'Paid' as paid__organic, -- funnel

FROM
  `sl-customers.raw_google_ads_mcc.ads_Campaign_7737157033` c
LEFT JOIN
  `sl-customers.raw_google_ads_mcc.ads_CampaignBasicStats_7737157033` cs
ON
  c.campaign_id = cs.campaign_id

LEFT JOIN(
  SELECT DISTINCT customer_id,customer_descriptive_name
   FROM`sl-customers.raw_google_ads_mcc.ads_Customer_7737157033`) ac ON c.customer_id = ac.customer_id 

LEFT JOIN (
  SELECT
    campaign_name,
    segments_date,
    MAX(REGEXP_EXTRACT(landing_page_view_unexpanded_final_url, r'utm_source=([^&]+)')) AS utm_source,
    MAX(REGEXP_EXTRACT(landing_page_view_unexpanded_final_url, r'utm_medium=([^&]+)')) AS utm_medium,
    MAX(REGEXP_EXTRACT(landing_page_view_unexpanded_final_url, r'utm_campaign=([^&]+)')) AS utm_campaign,
    MAX(REGEXP_EXTRACT(landing_page_view_unexpanded_final_url, r'utm_content=([^&]+)')) AS utm_content
  FROM
    `sl-customers.raw_google_ads_mcc.p_ads_LandingPageStats_7737157033`
  WHERE 
    landing_page_view_unexpanded_final_url IS NOT NULL
    AND REGEXP_EXTRACT(landing_page_view_unexpanded_final_url, r'utm_source=([^&]+)') IS NOT NULL
  GROUP BY
    campaign_name, segments_date
) ls 
ON ls.campaign_name = c.campaign_name AND ls.segments_date = cs.segments_date
LEFT JOIN (
  SELECT
    campaign_name,
    MAX(REGEXP_EXTRACT(landing_page_view_unexpanded_final_url, r'utm_source=([^&]+)')) AS utm_source,
    MAX(REGEXP_EXTRACT(landing_page_view_unexpanded_final_url, r'utm_medium=([^&]+)')) AS utm_medium,
    MAX(REGEXP_EXTRACT(landing_page_view_unexpanded_final_url, r'utm_campaign=([^&]+)')) AS utm_campaign,
    MAX(REGEXP_EXTRACT(landing_page_view_unexpanded_final_url, r'utm_content=([^&]+)')) AS utm_content
  FROM
    `sl-customers.raw_google_ads_mcc.p_ads_LandingPageStats_7737157033`
  WHERE 
    landing_page_view_unexpanded_final_url IS NOT NULL
    AND REGEXP_EXTRACT(landing_page_view_unexpanded_final_url, r'utm_source=([^&]+)') IS NOT NULL
    
  GROUP BY
    campaign_name
) ls_bridge
ON ls_bridge.campaign_name = c.campaign_name
WHERE 1=1
  AND c._DATA_DATE = c._LATEST_DATE
  AND cs.segments_date IS NOT NULL
  AND ac.customer_descriptive_name LIKE '%BoD%'
  
  

# temp
  # AND c.campaign_name = "_fd-240101 _fa-SL _fi- _fc-IT-it _fx-National Perform. _fy- _fe- _fp-G _ft-SE _fo-Finestre _fz-CO _fg-Leads _fk-"
   #and cs.segments_date = '2024-06-08'
# tempo

GROUP BY
  date, campaign_id, campaign_name, campaign_type_ads, utm_source, utm_medium, utm_campaign, utm_content,data_source_name
ORDER BY
  campaign_name DESC
