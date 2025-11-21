{{
  config(materialized='table')
}}

SELECT
    digit,
    speaker_name,
    MD5(CONCAT('speaker_', digit)) AS dynamics_speaker_id
FROM (VALUES 
    ('1', 'Ross'),
    ('2', 'Rachel'),
    ('3', 'Joey'),
    ('4', 'Monica'),
    ('5', 'Chandler'),
    ('6', 'Phoebe')
) AS mapping(digit, speaker_name)