{{ config(materialized='view') }}

SELECT PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', times) as dt, station, parameter, value FROM `festive-dolphin-366719.air_quality.air_quality`