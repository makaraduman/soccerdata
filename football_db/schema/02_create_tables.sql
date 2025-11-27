-- ============================================================
-- Football Statistics Database - Table Definitions
-- ============================================================
-- This script creates all tables for storing football statistics
-- Run after 01_create_database.sql

\c football_stats;
SET search_path TO football, public;

-- ============================================================
-- 1. LEAGUES TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS leagues (
    league_id SERIAL PRIMARY KEY,
    league_name VARCHAR(100) NOT NULL,
    country VARCHAR(50) NOT NULL,
    league_code VARCHAR(50) UNIQUE NOT NULL,
    tier INTEGER DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_league UNIQUE(country, league_name)
);

CREATE INDEX idx_leagues_code ON leagues(league_code);
CREATE INDEX idx_leagues_country ON leagues(country);

COMMENT ON TABLE leagues IS 'Reference table for football leagues';
COMMENT ON COLUMN leagues.league_code IS 'Standardized league code (e.g., ENG-Premier League)';

-- ============================================================
-- 2. SEASONS TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS seasons (
    season_id SERIAL PRIMARY KEY,
    season_name VARCHAR(20) NOT NULL UNIQUE,
    start_year INTEGER NOT NULL,
    end_year INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT check_season_years CHECK (end_year >= start_year)
);

CREATE INDEX idx_seasons_year ON seasons(start_year, end_year);

COMMENT ON TABLE seasons IS 'Reference table for football seasons';
COMMENT ON COLUMN seasons.season_name IS 'Season identifier (e.g., 2020-2021, 2021-2022)';

-- ============================================================
-- 3. TEAMS TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS teams (
    team_id SERIAL PRIMARY KEY,
    team_name VARCHAR(100) NOT NULL,
    team_name_alt VARCHAR(100),
    country VARCHAR(50),
    founded_year INTEGER,
    stadium_name VARCHAR(100),
    stadium_capacity INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_teams_name ON teams(team_name);
CREATE INDEX idx_teams_name_trgm ON teams USING gin (team_name gin_trgm_ops);
CREATE INDEX idx_teams_country ON teams(country);

COMMENT ON TABLE teams IS 'Reference table for football teams';
COMMENT ON COLUMN teams.team_name_alt IS 'Alternative team name for matching across data sources';

-- ============================================================
-- 4. LEAGUE_SEASONS TABLE (Junction table)
-- ============================================================
CREATE TABLE IF NOT EXISTS league_seasons (
    league_season_id SERIAL PRIMARY KEY,
    league_id INTEGER NOT NULL REFERENCES leagues(league_id) ON DELETE CASCADE,
    season_id INTEGER NOT NULL REFERENCES seasons(season_id) ON DELETE CASCADE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_league_season UNIQUE(league_id, season_id)
);

CREATE INDEX idx_league_seasons_league ON league_seasons(league_id);
CREATE INDEX idx_league_seasons_season ON league_seasons(season_id);

COMMENT ON TABLE league_seasons IS 'Junction table linking leagues and seasons';

-- ============================================================
-- 5. MATCHES TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS matches (
    match_id SERIAL PRIMARY KEY,
    league_season_id INTEGER NOT NULL REFERENCES league_seasons(league_season_id) ON DELETE CASCADE,
    home_team_id INTEGER NOT NULL REFERENCES teams(team_id) ON DELETE CASCADE,
    away_team_id INTEGER NOT NULL REFERENCES teams(team_id) ON DELETE CASCADE,
    match_date TIMESTAMP WITH TIME ZONE NOT NULL,
    matchweek INTEGER,
    home_score INTEGER,
    away_score INTEGER,
    home_halftime_score INTEGER,
    away_halftime_score INTEGER,
    attendance INTEGER,
    venue VARCHAR(100),
    referee VARCHAR(100),
    match_status VARCHAR(20) DEFAULT 'scheduled',
    external_match_id VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT check_different_teams CHECK (home_team_id != away_team_id),
    CONSTRAINT check_scores CHECK (
        (home_score IS NULL AND away_score IS NULL) OR
        (home_score IS NOT NULL AND away_score IS NOT NULL)
    )
);

CREATE INDEX idx_matches_date ON matches(match_date);
CREATE INDEX idx_matches_league_season ON matches(league_season_id);
CREATE INDEX idx_matches_home_team ON matches(home_team_id);
CREATE INDEX idx_matches_away_team ON matches(away_team_id);
CREATE INDEX idx_matches_status ON matches(match_status);
CREATE INDEX idx_matches_external_id ON matches(external_match_id);
CREATE UNIQUE INDEX idx_matches_unique ON matches(league_season_id, home_team_id, away_team_id, match_date);

COMMENT ON TABLE matches IS 'Main table for match fixtures and results';
COMMENT ON COLUMN matches.match_status IS 'Status: scheduled, live, completed, postponed, cancelled';
COMMENT ON COLUMN matches.external_match_id IS 'External ID from data source for deduplication';

-- ============================================================
-- 6. TEAM_SEASON_STATS TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS team_season_stats (
    team_season_stat_id SERIAL PRIMARY KEY,
    team_id INTEGER NOT NULL REFERENCES teams(team_id) ON DELETE CASCADE,
    league_season_id INTEGER NOT NULL REFERENCES league_seasons(league_season_id) ON DELETE CASCADE,
    stat_type VARCHAR(50) NOT NULL,

    -- General stats
    matches_played INTEGER DEFAULT 0,
    wins INTEGER DEFAULT 0,
    draws INTEGER DEFAULT 0,
    losses INTEGER DEFAULT 0,
    goals_for INTEGER DEFAULT 0,
    goals_against INTEGER DEFAULT 0,

    -- Possession and passing
    possession_pct DECIMAL(5,2),
    passes_completed INTEGER,
    passes_attempted INTEGER,
    pass_completion_pct DECIMAL(5,2),

    -- Shooting
    shots INTEGER,
    shots_on_target INTEGER,
    shots_on_target_pct DECIMAL(5,2),

    -- Defensive
    tackles INTEGER,
    tackles_won INTEGER,
    interceptions INTEGER,
    blocks INTEGER,
    clearances INTEGER,

    -- Discipline
    yellow_cards INTEGER DEFAULT 0,
    red_cards INTEGER DEFAULT 0,
    fouls_committed INTEGER,
    fouls_drawn INTEGER,

    -- Additional stats (JSON for flexibility)
    additional_stats JSONB,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_team_season_stat UNIQUE(team_id, league_season_id, stat_type)
);

CREATE INDEX idx_team_season_stats_team ON team_season_stats(team_id);
CREATE INDEX idx_team_season_stats_league_season ON team_season_stats(league_season_id);
CREATE INDEX idx_team_season_stats_type ON team_season_stats(stat_type);
CREATE INDEX idx_team_season_stats_additional ON team_season_stats USING gin (additional_stats);

COMMENT ON TABLE team_season_stats IS 'Season-level statistics for teams';
COMMENT ON COLUMN team_season_stats.stat_type IS 'Type of stats: standard, passing, shooting, defensive, etc.';

-- ============================================================
-- 7. PLAYERS TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS players (
    player_id SERIAL PRIMARY KEY,
    player_name VARCHAR(100) NOT NULL,
    birth_date DATE,
    nationality VARCHAR(50),
    position VARCHAR(20),
    height_cm INTEGER,
    weight_kg INTEGER,
    preferred_foot VARCHAR(10),
    external_player_id VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_players_name ON players(player_name);
CREATE INDEX idx_players_name_trgm ON players USING gin (player_name gin_trgm_ops);
CREATE INDEX idx_players_nationality ON players(nationality);
CREATE INDEX idx_players_position ON players(position);
CREATE INDEX idx_players_external_id ON players(external_player_id);

COMMENT ON TABLE players IS 'Reference table for football players';
COMMENT ON COLUMN players.position IS 'Position: GK, DF, MF, FW';
COMMENT ON COLUMN players.external_player_id IS 'External ID from data source';

-- ============================================================
-- 8. PLAYER_SEASON_STATS TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS player_season_stats (
    player_season_stat_id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL REFERENCES players(player_id) ON DELETE CASCADE,
    team_id INTEGER NOT NULL REFERENCES teams(team_id) ON DELETE CASCADE,
    league_season_id INTEGER NOT NULL REFERENCES league_seasons(league_season_id) ON DELETE CASCADE,
    stat_type VARCHAR(50) NOT NULL,

    -- Appearance stats
    matches_played INTEGER DEFAULT 0,
    starts INTEGER DEFAULT 0,
    minutes_played INTEGER DEFAULT 0,

    -- Performance stats
    goals INTEGER DEFAULT 0,
    assists INTEGER DEFAULT 0,
    penalty_goals INTEGER DEFAULT 0,
    penalty_attempts INTEGER DEFAULT 0,

    -- Shooting
    shots INTEGER,
    shots_on_target INTEGER,
    shots_on_target_pct DECIMAL(5,2),
    goals_per_shot DECIMAL(5,3),

    -- Passing
    passes_completed INTEGER,
    passes_attempted INTEGER,
    pass_completion_pct DECIMAL(5,2),
    key_passes INTEGER,

    -- Defensive
    tackles INTEGER,
    tackles_won INTEGER,
    interceptions INTEGER,
    blocks INTEGER,
    clearances INTEGER,

    -- Discipline
    yellow_cards INTEGER DEFAULT 0,
    red_cards INTEGER DEFAULT 0,
    fouls_committed INTEGER,
    fouls_drawn INTEGER,

    -- Goalkeeper specific (if applicable)
    saves INTEGER,
    saves_pct DECIMAL(5,2),
    clean_sheets INTEGER,
    goals_against INTEGER,

    -- Additional stats (JSON for flexibility)
    additional_stats JSONB,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_player_season_stat UNIQUE(player_id, team_id, league_season_id, stat_type)
);

CREATE INDEX idx_player_season_stats_player ON player_season_stats(player_id);
CREATE INDEX idx_player_season_stats_team ON player_season_stats(team_id);
CREATE INDEX idx_player_season_stats_league_season ON player_season_stats(league_season_id);
CREATE INDEX idx_player_season_stats_type ON player_season_stats(stat_type);
CREATE INDEX idx_player_season_stats_additional ON player_season_stats USING gin (additional_stats);

COMMENT ON TABLE player_season_stats IS 'Season-level statistics for players';
COMMENT ON COLUMN player_season_stats.stat_type IS 'Type of stats: standard, passing, shooting, defensive, goalkeeping, etc.';

-- ============================================================
-- 9. LEAGUE_STANDINGS TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS league_standings (
    standing_id SERIAL PRIMARY KEY,
    league_season_id INTEGER NOT NULL REFERENCES league_seasons(league_season_id) ON DELETE CASCADE,
    team_id INTEGER NOT NULL REFERENCES teams(team_id) ON DELETE CASCADE,
    position INTEGER NOT NULL,
    matches_played INTEGER DEFAULT 0,
    wins INTEGER DEFAULT 0,
    draws INTEGER DEFAULT 0,
    losses INTEGER DEFAULT 0,
    goals_for INTEGER DEFAULT 0,
    goals_against INTEGER DEFAULT 0,
    goal_difference INTEGER DEFAULT 0,
    points INTEGER DEFAULT 0,
    home_wins INTEGER DEFAULT 0,
    home_draws INTEGER DEFAULT 0,
    home_losses INTEGER DEFAULT 0,
    away_wins INTEGER DEFAULT 0,
    away_draws INTEGER DEFAULT 0,
    away_losses INTEGER DEFAULT 0,
    standing_date DATE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_standing UNIQUE(league_season_id, team_id, standing_date)
);

CREATE INDEX idx_standings_league_season ON league_standings(league_season_id);
CREATE INDEX idx_standings_team ON league_standings(team_id);
CREATE INDEX idx_standings_date ON league_standings(standing_date);
CREATE INDEX idx_standings_position ON league_standings(position);

COMMENT ON TABLE league_standings IS 'League table/standings snapshots';
COMMENT ON COLUMN league_standings.standing_date IS 'Date of this standings snapshot';

-- ============================================================
-- 10. MATCH_EVENTS TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS match_events (
    event_id SERIAL PRIMARY KEY,
    match_id INTEGER NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
    event_type VARCHAR(30) NOT NULL,
    event_minute INTEGER NOT NULL,
    extra_minute INTEGER,
    player_id INTEGER REFERENCES players(player_id) ON DELETE SET NULL,
    team_id INTEGER NOT NULL REFERENCES teams(team_id) ON DELETE CASCADE,

    -- For goals
    assist_player_id INTEGER REFERENCES players(player_id) ON DELETE SET NULL,
    goal_type VARCHAR(30),

    -- For cards
    card_type VARCHAR(10),

    -- For substitutions
    player_out_id INTEGER REFERENCES players(player_id) ON DELETE SET NULL,
    player_in_id INTEGER REFERENCES players(player_id) ON DELETE SET NULL,

    -- Additional event data
    event_details JSONB,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT check_event_type CHECK (event_type IN ('goal', 'yellow_card', 'red_card', 'substitution', 'penalty_missed', 'own_goal', 'var_decision'))
);

CREATE INDEX idx_match_events_match ON match_events(match_id);
CREATE INDEX idx_match_events_player ON match_events(player_id);
CREATE INDEX idx_match_events_team ON match_events(team_id);
CREATE INDEX idx_match_events_type ON match_events(event_type);
CREATE INDEX idx_match_events_minute ON match_events(event_minute);

COMMENT ON TABLE match_events IS 'Individual events within matches (goals, cards, substitutions)';
COMMENT ON COLUMN match_events.event_minute IS 'Minute when event occurred';
COMMENT ON COLUMN match_events.extra_minute IS 'Additional/injury time minutes';
COMMENT ON COLUMN match_events.goal_type IS 'Type: regular, penalty, own_goal, free_kick, header, etc.';

-- ============================================================
-- 11. DATA_SOURCES TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS data_sources (
    source_id SERIAL PRIMARY KEY,
    source_name VARCHAR(50) NOT NULL UNIQUE,
    source_url VARCHAR(200),
    rate_limit_seconds INTEGER DEFAULT 3,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_data_sources_active ON data_sources(is_active);

COMMENT ON TABLE data_sources IS 'Reference table for data sources used in scraping';
COMMENT ON COLUMN data_sources.rate_limit_seconds IS 'Minimum seconds between requests to this source';

-- ============================================================
-- 12. DATA_LOAD_LOG TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS data_load_log (
    load_id SERIAL PRIMARY KEY,
    source_id INTEGER REFERENCES data_sources(source_id) ON DELETE SET NULL,
    load_type VARCHAR(50) NOT NULL,
    target_table VARCHAR(50) NOT NULL,
    league_season_id INTEGER REFERENCES league_seasons(league_season_id) ON DELETE SET NULL,
    load_start TIMESTAMP WITH TIME ZONE NOT NULL,
    load_end TIMESTAMP WITH TIME ZONE,
    records_processed INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    status VARCHAR(20) NOT NULL,
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT check_load_status CHECK (status IN ('running', 'completed', 'failed', 'partial'))
);

CREATE INDEX idx_data_load_log_source ON data_load_log(source_id);
CREATE INDEX idx_data_load_log_type ON data_load_log(load_type);
CREATE INDEX idx_data_load_log_table ON data_load_log(target_table);
CREATE INDEX idx_data_load_log_start ON data_load_log(load_start);
CREATE INDEX idx_data_load_log_status ON data_load_log(status);

COMMENT ON TABLE data_load_log IS 'Audit log for all data loading operations';
COMMENT ON COLUMN data_load_log.load_type IS 'Type: initial_load, daily_update, backfill, etc.';
COMMENT ON COLUMN data_load_log.status IS 'Status: running, completed, failed, partial';

-- ============================================================
-- Create update timestamp trigger function
-- ============================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply trigger to tables with updated_at column
CREATE TRIGGER update_leagues_updated_at BEFORE UPDATE ON leagues
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_teams_updated_at BEFORE UPDATE ON teams
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_matches_updated_at BEFORE UPDATE ON matches
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_team_season_stats_updated_at BEFORE UPDATE ON team_season_stats
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_players_updated_at BEFORE UPDATE ON players
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_player_season_stats_updated_at BEFORE UPDATE ON player_season_stats
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_league_standings_updated_at BEFORE UPDATE ON league_standings
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_data_sources_updated_at BEFORE UPDATE ON data_sources
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
