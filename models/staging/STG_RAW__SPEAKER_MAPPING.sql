{{ config(
    materialized='table',
    unique_key='speaker_id'
) }}

SELECT
    digit,
    speaker_name,
    MD5(UPPER(TRIM(speaker_name))) AS speaker_id
FROM (VALUES 
    ('1', 'Ross'),
    ('2', 'Rachel'),
    ('3', 'Joey'),
    ('4', 'Monica'),
    ('5', 'Chandler'),
    ('6', 'Phoebe')
) AS mapping(digit, speaker_name)
