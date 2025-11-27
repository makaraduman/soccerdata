# Football Statistics Database

A comprehensive PostgreSQL database solution for storing and managing football (soccer) statistics from top European leagues. This project uses the [soccerdata](https://github.com/probberechts/soccerdata) library to scrape and parse data from FBref and other sources.

## Features

- **Comprehensive Data Storage**: Match results, team statistics, player statistics, and league standings
- **Multiple Data Sources**: Primarily uses FBref with support for other sources
- **Historical Data**: Load data from 2020-2021 season onwards
- **Daily Updates**: Automated scripts for daily data updates
- **Robust Architecture**: Proper indexing, error handling, and logging
- **Data Deduplication**: Prevents duplicate entries with proper constraints

## Supported Leagues

- **England**: Premier League
- **Italy**: Serie A
- **France**: Ligue 1
- **Germany**: Bundesliga
- **Spain**: La Liga

## Project Structure

```
football_db/
├── config/              # Configuration files
│   └── db_config.yaml.example
├── schema/              # Database schema DDL files
│   ├── 00_setup_database.sh
│   ├── 01_create_database.sql
│   ├── 02_create_tables.sql
│   └── 03_insert_reference_data.sql
├── scripts/             # Python data loading scripts
│   ├── db_utils.py
│   ├── logging_utils.py
│   ├── load_matches.py
│   ├── load_team_stats.py
│   ├── load_player_stats.py
│   ├── load_standings.py
│   ├── load_historical_data.py
│   └── daily_update.py
├── logs/                # Log files (created automatically)
├── docs/                # Documentation
└── requirements.txt     # Python dependencies
```

## Database Schema

The database consists of the following main tables:

### Reference Tables
- **leagues**: Football leagues
- **seasons**: Football seasons
- **teams**: Team information
- **players**: Player information
- **data_sources**: Data source tracking

### Data Tables
- **matches**: Match fixtures and results
- **team_season_stats**: Season-level team statistics
- **player_season_stats**: Season-level player statistics
- **league_standings**: League table snapshots
- **match_events**: Match events (goals, cards, substitutions)

### Tracking Tables
- **league_seasons**: Junction table for league-season combinations
- **data_load_log**: Audit log for data loading operations

## Setup Instructions

### Prerequisites

1. **PostgreSQL** (version 12 or higher)
   ```bash
   # Ubuntu/Debian
   sudo apt-get install postgresql postgresql-contrib

   # macOS
   brew install postgresql
   ```

2. **Python** (version 3.9 or higher)
   ```bash
   python --version
   ```

### Step 1: Install Python Dependencies

```bash
cd football_db
pip install -r requirements.txt
```

### Step 2: Configure Database Connection

1. Copy the example configuration file:
   ```bash
   cp config/db_config.yaml.example config/db_config.yaml
   ```

2. Edit `config/db_config.yaml` with your PostgreSQL credentials:
   ```yaml
   database:
     host: localhost
     port: 5432
     database: football_stats
     user: your_username
     password: your_password
     schema: football
   ```

### Step 3: Create Database Schema

Run the setup script to create the database and all tables:

```bash
cd schema
chmod +x 00_setup_database.sh
./00_setup_database.sh localhost 5432 postgres
```

Or run SQL files manually:
```bash
psql -h localhost -U postgres -f 01_create_database.sql
psql -h localhost -U postgres -d football_stats -f 02_create_tables.sql
psql -h localhost -U postgres -d football_stats -f 03_insert_reference_data.sql
```

### Step 4: Test Database Connection

```bash
cd ../scripts
python db_utils.py
```

You should see: "Database connection test successful!"

## Usage

### Loading Historical Data

Load all historical data from 2020-2021 onwards:

```bash
cd scripts
python load_historical_data.py
```

**Options:**
```bash
# Load specific league
python load_historical_data.py --leagues "ENG-Premier League"

# Load specific season range
python load_historical_data.py --start-season 2022-2023 --end-season 2023-2024

# Load only matches and standings (skip stats)
python load_historical_data.py --skip-team-stats --skip-player-stats

# Force refresh cached data
python load_historical_data.py --force-refresh
```

### Daily Updates

Update the database with latest data:

```bash
python daily_update.py
```

**Options:**
```bash
# Update specific leagues
python daily_update.py --leagues "ENG-Premier League" "ESP-La Liga"

# Update specific season
python daily_update.py --season 2024-2025

# Update only matches
python daily_update.py --no-stats --no-standings
```

### Loading Individual Data Types

**Matches:**
```bash
python load_matches.py --league "ENG-Premier League" --season 2023-2024
```

**Team Statistics:**
```bash
python load_team_stats.py --league "ENG-Premier League" --season 2023-2024
```

**Player Statistics:**
```bash
python load_player_stats.py --league "ENG-Premier League" --season 2023-2024
```

**League Standings:**
```bash
python load_standings.py --league "ENG-Premier League" --season 2023-2024
```

## Querying the Database

### Example Queries

**Get all matches for a team:**
```sql
SELECT
    m.match_date,
    ht.team_name as home_team,
    at.team_name as away_team,
    m.home_score,
    m.away_score
FROM football.matches m
JOIN football.teams ht ON m.home_team_id = ht.team_id
JOIN football.teams at ON m.away_team_id = at.team_id
WHERE ht.team_name = 'Manchester City' OR at.team_name = 'Manchester City'
ORDER BY m.match_date DESC;
```

**Get current league standings:**
```sql
SELECT
    ls.position,
    t.team_name,
    ls.matches_played,
    ls.wins,
    ls.draws,
    ls.losses,
    ls.goals_for,
    ls.goals_against,
    ls.goal_difference,
    ls.points
FROM football.league_standings ls
JOIN football.teams t ON ls.team_id = t.team_id
JOIN football.league_seasons lsn ON ls.league_season_id = lsn.league_season_id
JOIN football.leagues l ON lsn.league_id = l.league_id
WHERE l.league_code = 'ENG-Premier League'
    AND ls.standing_date = (
        SELECT MAX(standing_date)
        FROM football.league_standings
        WHERE league_season_id = lsn.league_season_id
    )
ORDER BY ls.position;
```

**Get top scorers:**
```sql
SELECT
    p.player_name,
    t.team_name,
    ps.goals,
    ps.assists,
    ps.matches_played
FROM football.player_season_stats ps
JOIN football.players p ON ps.player_id = p.player_id
JOIN football.teams t ON ps.team_id = t.team_id
JOIN football.league_seasons ls ON ps.league_season_id = ls.league_season_id
JOIN football.leagues l ON ls.league_id = l.league_id
WHERE l.league_code = 'ENG-Premier League'
    AND ps.stat_type = 'standard'
ORDER BY ps.goals DESC
LIMIT 20;
```

## Scheduling Daily Updates

### Using Cron (Linux/macOS)

Add to crontab:
```bash
crontab -e
```

Add this line to run daily at 2 AM:
```
0 2 * * * cd /path/to/soccerdata/football_db/scripts && /usr/bin/python3 daily_update.py >> /path/to/logs/daily_update_cron.log 2>&1
```

### Using Windows Task Scheduler

1. Open Task Scheduler
2. Create Basic Task
3. Set trigger to daily at preferred time
4. Action: Start a program
5. Program: `python`
6. Arguments: `/path/to/football_db/scripts/daily_update.py`

## Data Sources and Rate Limiting

The system uses the **FBref** data source primarily, which has the following characteristics:

- **Rate Limit**: 7 seconds between requests (configured in db_config.yaml)
- **Data Coverage**: Comprehensive statistics for all top 5 European leagues
- **Update Frequency**: Daily updates recommended
- **Caching**: The soccerdata library caches data locally to minimize requests

**Important**: Please respect the terms of service of data sources. This tool is for personal, educational, or research purposes.

## Logging

All operations are logged to:
- **Console**: INFO level and above
- **Log Files**:
  - `logs/football_db.log`: All operations (DEBUG level)
  - `logs/football_db_errors.log`: Errors only
  - Database: `data_load_log` table tracks all load operations

## Troubleshooting

### Database Connection Issues

**Error**: "could not connect to server"
```bash
# Check if PostgreSQL is running
sudo systemctl status postgresql  # Linux
brew services list  # macOS

# Check if correct credentials in config/db_config.yaml
```

### Import Errors

**Error**: "No module named 'soccerdata'"
```bash
# Make sure you're in the soccerdata repository
cd /path/to/soccerdata/football_db/scripts
python load_historical_data.py
```

### Empty Data Returns

**Issue**: No data loaded for specific league/season
- Check if the league code is correct (e.g., 'ENG-Premier League' not 'Premier League')
- Verify the season format (e.g., '2023-2024' not '2023')
- Try with `--force-refresh` flag to bypass cache
- Check FBref website to ensure data is available

### Rate Limiting

**Error**: Too many requests
- Increase `rate_limit_seconds` in config/db_config.yaml
- Add delays between operations
- The system already includes built-in rate limiting

## Performance Optimization

### Database Indexes

All critical columns are indexed for performance:
- Team and player lookups
- Date-based queries
- League/season filtering

### Bulk Operations

Scripts use `execute_values` for bulk inserts, significantly faster than individual inserts.

### Connection Pooling

Database connections are pooled to reduce overhead.

## Future Enhancements

Planned features for future releases:

- [ ] Match event tracking (goals, cards, substitutions)
- [ ] Advanced analytics views
- [ ] API layer for data access
- [ ] Web dashboard for visualization
- [ ] Additional data sources (Understat, WhoScored)
- [ ] Machine learning integration
- [ ] Export functionality (CSV, JSON)

## Contributing

This is a personal/research project. If you have suggestions:
1. Document the enhancement
2. Test thoroughly
3. Ensure it doesn't violate data source terms of service

## License

This project uses the [soccerdata](https://github.com/probberechts/soccerdata) library, which is licensed under Apache 2.0.

**Usage Notice**: Please use this web scraping tool responsibly and in compliance with the terms of service of the websites being scraped. The software is provided as-is, without any warranty or guarantees of any kind.

## Support

For issues related to:
- **Database schema**: Check the DDL files in `schema/`
- **Data loading**: Check logs in `logs/` directory
- **soccerdata library**: See [soccerdata documentation](https://soccerdata.readthedocs.io/)

## Acknowledgments

- [soccerdata](https://github.com/probberechts/soccerdata) by Pieter Robberechts for the excellent data scraping library
- [FBref](https://fbref.com/) for comprehensive football statistics
- PostgreSQL community for the robust database system
