{{
  config(
    materialized='table',
    unique_key='speaker_id'
  )
}}

SELECT DISTINCT
    --Creo md5 para crear el id
    MD5(TRIM(UPPER(speaker))) AS speaker_id,
    TRIM(speaker) AS speaker_name

FROM {{ source('raw', 'raw_dialogs') }}
WHERE speaker IS NOT NULL 
    AND TRIM(speaker) != ''  --consulta para ver que es