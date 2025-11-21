{{
  config(
    materialized='table',
    unique_key='emotion_id'
  )
}}

SELECT DISTINCT
    MD5(TRIM(UPPER(emotion))) AS emotion_id,
    TRIM(emotion) AS emotion_name
FROM {{ source('raw', 'raw_emotions') }}
WHERE emotion IS NOT NULL
  AND TRIM(emotion) != ''