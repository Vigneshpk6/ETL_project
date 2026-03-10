-- =============================================================
-- analysis_queries.sql
-- Ready-to-run analytical queries for the weather_db database
-- Run these in MySQL Workbench, DBeaver, or via the CLI:
--   mysql -u etl_user -p weather_db < analysis_queries.sql
-- =============================================================

USE weather_db;

-- ──────────────────────────────────────────────────────────────
-- 1. Overview: latest ingested snapshot per city
-- ──────────────────────────────────────────────────────────────
SELECT
    city,
    MAX(observed_at)        AS latest_obs,
    COUNT(*)                AS total_rows,
    ROUND(AVG(temp_c), 1)   AS overall_avg_temp_c,
    ROUND(MIN(temp_c), 1)   AS record_low_c,
    ROUND(MAX(temp_c), 1)   AS record_high_c
FROM weather_observations
GROUP BY city
ORDER BY city;


-- ──────────────────────────────────────────────────────────────
-- 2. Daily highs, lows, and rainfall (view)
-- ──────────────────────────────────────────────────────────────
SELECT *
FROM vw_daily_summary
ORDER BY city, obs_date DESC
LIMIT 50;


-- ──────────────────────────────────────────────────────────────
-- 3. City-level comparison (view)
-- ──────────────────────────────────────────────────────────────
SELECT *
FROM vw_city_comparison
ORDER BY avg_temp_c DESC;


-- ──────────────────────────────────────────────────────────────
-- 4. Extreme weather alerts (view)
-- ──────────────────────────────────────────────────────────────
SELECT city, observed_at, alert_type, temp_c,
       wind_speed_kph, precipitation_mm, uv_index
FROM vw_weather_alerts
ORDER BY observed_at DESC
LIMIT 30;


-- ──────────────────────────────────────────────────────────────
-- 5. Hourly temperature trend for a single city (last 48 hrs)
-- ──────────────────────────────────────────────────────────────
SELECT
    observed_at,
    temp_c,
    apparent_temp_c,
    humidity_pct,
    weather_desc
FROM weather_observations
WHERE city = 'London'
  AND observed_at >= NOW() - INTERVAL 48 HOUR
ORDER BY observed_at;


-- ──────────────────────────────────────────────────────────────
-- 6. Wind rose: frequency of each cardinal direction
-- ──────────────────────────────────────────────────────────────
SELECT
    wind_cardinal,
    COUNT(*)                        AS frequency,
    ROUND(AVG(wind_speed_kph), 1)   AS avg_speed_kph,
    ROUND(MAX(wind_speed_kph), 1)   AS max_speed_kph
FROM weather_observations
WHERE wind_cardinal IS NOT NULL
GROUP BY wind_cardinal
ORDER BY frequency DESC;


-- ──────────────────────────────────────────────────────────────
-- 7. Precipitation days per city (where rain > 0.1 mm)
-- ──────────────────────────────────────────────────────────────
SELECT
    city,
    COUNT(DISTINCT DATE(observed_at)) AS rainy_days,
    ROUND(SUM(precipitation_mm), 1)   AS total_precip_mm,
    ROUND(AVG(NULLIF(precipitation_mm, 0)), 2) AS avg_rainy_hour_mm
FROM weather_observations
WHERE precipitation_mm > 0.1
GROUP BY city
ORDER BY total_precip_mm DESC;


-- ──────────────────────────────────────────────────────────────
-- 8. UV index summary (daytime hours only)
-- ──────────────────────────────────────────────────────────────
SELECT
    city,
    ROUND(AVG(uv_index), 2)    AS avg_uv,
    ROUND(MAX(uv_index), 2)    AS max_uv,
    uv_category,
    COUNT(*)                   AS hours
FROM weather_observations
WHERE is_daytime = TRUE
  AND uv_index IS NOT NULL
GROUP BY city, uv_category
ORDER BY city, max_uv DESC;


-- ──────────────────────────────────────────────────────────────
-- 9. Season breakdown — average conditions per season
-- ──────────────────────────────────────────────────────────────
SELECT
    city,
    season,
    ROUND(AVG(temp_c), 1)           AS avg_temp_c,
    ROUND(AVG(humidity_pct), 1)     AS avg_humidity_pct,
    ROUND(SUM(precipitation_mm), 1) AS total_precip_mm,
    COUNT(*)                        AS observations
FROM weather_observations
GROUP BY city, season
ORDER BY city, season;


-- ──────────────────────────────────────────────────────────────
-- 10. Beaufort scale distribution per city
-- ──────────────────────────────────────────────────────────────
SELECT
    city,
    beaufort_scale,
    CASE beaufort_scale
        WHEN 0  THEN 'Calm'
        WHEN 1  THEN 'Light Air'
        WHEN 2  THEN 'Light Breeze'
        WHEN 3  THEN 'Gentle Breeze'
        WHEN 4  THEN 'Moderate Breeze'
        WHEN 5  THEN 'Fresh Breeze'
        WHEN 6  THEN 'Strong Breeze'
        WHEN 7  THEN 'Near Gale'
        WHEN 8  THEN 'Gale'
        WHEN 9  THEN 'Strong Gale'
        WHEN 10 THEN 'Storm'
        WHEN 11 THEN 'Violent Storm'
        WHEN 12 THEN 'Hurricane Force'
    END                 AS beaufort_description,
    COUNT(*)            AS hours,
    ROUND(AVG(wind_speed_kph), 1) AS avg_kph
FROM weather_observations
WHERE beaufort_scale IS NOT NULL
GROUP BY city, beaufort_scale
ORDER BY city, beaufort_scale;
