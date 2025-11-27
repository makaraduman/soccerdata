-- ============================================================
-- Football Statistics Database - Reference Data
-- ============================================================
-- This script inserts initial reference data
-- Run after 02_create_tables.sql

\c football_stats;
SET search_path TO football, public;

-- ============================================================
-- Insert Leagues (Top 5 European Leagues)
-- ============================================================
INSERT INTO leagues (league_name, country, league_code, tier) VALUES
    ('Premier League', 'England', 'ENG-Premier League', 1),
    ('Serie A', 'Italy', 'ITA-Serie A', 1),
    ('Ligue 1', 'France', 'FRA-Ligue 1', 1),
    ('Bundesliga', 'Germany', 'GER-Bundesliga', 1),
    ('La Liga', 'Spain', 'ESP-La Liga', 1)
ON CONFLICT (league_code) DO NOTHING;

-- ============================================================
-- Insert Seasons (2020-2021 to 2024-2025)
-- ============================================================
INSERT INTO seasons (season_name, start_year, end_year) VALUES
    ('2020-2021', 2020, 2021),
    ('2021-2022', 2021, 2022),
    ('2022-2023', 2022, 2023),
    ('2023-2024', 2023, 2024),
    ('2024-2025', 2024, 2025)
ON CONFLICT (season_name) DO NOTHING;

-- ============================================================
-- Insert Data Sources
-- ============================================================
INSERT INTO data_sources (source_name, source_url, rate_limit_seconds, is_active) VALUES
    ('FBref', 'https://fbref.com', 7, true),
    ('Understat', 'https://understat.com', 3, true),
    ('ESPN', 'https://www.espn.com/soccer/', 3, true),
    ('WhoScored', 'https://www.whoscored.com', 5, true),
    ('FotMob', 'https://fotmob.com', 3, true),
    ('Sofascore', 'https://www.sofascore.com', 3, true)
ON CONFLICT (source_name) DO NOTHING;

-- ============================================================
-- Create League-Season Combinations
-- ============================================================
INSERT INTO league_seasons (league_id, season_id)
SELECT l.league_id, s.season_id
FROM leagues l
CROSS JOIN seasons s
WHERE s.start_year >= 2020
ON CONFLICT (league_id, season_id) DO NOTHING;

-- ============================================================
-- Display inserted data
-- ============================================================
SELECT 'Leagues inserted:' as info;
SELECT league_id, league_code, league_name, country FROM leagues ORDER BY country;

SELECT 'Seasons inserted:' as info;
SELECT season_id, season_name, start_year, end_year FROM seasons ORDER BY start_year;

SELECT 'Data sources inserted:' as info;
SELECT source_id, source_name, rate_limit_seconds, is_active FROM data_sources ORDER BY source_id;

SELECT 'League-Season combinations created:' as info;
SELECT COUNT(*) as total_combinations FROM league_seasons;
