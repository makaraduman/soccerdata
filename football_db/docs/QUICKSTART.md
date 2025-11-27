# Quick Start Guide

This guide will help you get the Football Statistics Database up and running in 15 minutes.

## Prerequisites

- PostgreSQL 12+ installed and running
- Python 3.9+ installed
- Basic command line knowledge

## Step-by-Step Setup

### 1. Install Dependencies (2 minutes)

```bash
cd football_db
pip install -r requirements.txt
```

### 2. Configure Database (3 minutes)

```bash
# Copy configuration template
cp config/db_config.yaml.example config/db_config.yaml

# Edit with your PostgreSQL credentials
nano config/db_config.yaml  # or use your preferred editor
```

Update these fields:
```yaml
database:
  host: localhost
  port: 5432
  database: football_stats
  user: YOUR_USERNAME
  password: YOUR_PASSWORD
```

### 3. Create Database Schema (2 minutes)

```bash
cd schema
chmod +x 00_setup_database.sh

# Run setup script (you'll be prompted for PostgreSQL password)
./00_setup_database.sh localhost 5432 postgres
```

### 4. Test Connection (1 minute)

```bash
cd ../scripts
python db_utils.py
```

Expected output: "Database connection test successful!"

### 5. Load Initial Data (5-10 minutes)

Start with one league and one season to test:

```bash
# Load Premier League 2023-2024 season
python load_matches.py --league "ENG-Premier League" --season 2023-2024
python load_team_stats.py --league "ENG-Premier League" --season 2023-2024
python load_standings.py --league "ENG-Premier League" --season 2023-2024
```

### 6. Query Your Data

Connect to PostgreSQL and try a query:

```bash
psql -h localhost -U postgres -d football_stats
```

```sql
-- Set schema
SET search_path TO football, public;

-- View loaded matches
SELECT
    m.match_date,
    ht.team_name as home_team,
    at.team_name as away_team,
    m.home_score,
    m.away_score
FROM matches m
JOIN teams ht ON m.home_team_id = ht.team_id
JOIN teams at ON m.away_team_id = at.team_id
ORDER BY m.match_date DESC
LIMIT 10;
```

## Next Steps

### Load Historical Data

Once you've tested with one season, load all historical data:

```bash
cd scripts
python load_historical_data.py
```

This will load all 5 leagues from 2020-2021 onwards. **Note**: This may take 1-2 hours due to rate limiting.

### Set Up Daily Updates

After loading historical data, set up daily updates:

```bash
# Test daily update
python daily_update.py

# If successful, set up cron job (Linux/macOS)
crontab -e
```

Add this line:
```
0 2 * * * cd /path/to/football_db/scripts && python daily_update.py
```

## Common Issues

### "Permission denied" when running setup script
```bash
chmod +x schema/00_setup_database.sh
```

### "No module named 'psycopg2'"
```bash
pip install psycopg2-binary
```

### "Database does not exist"
Create it manually:
```bash
psql -h localhost -U postgres -c "CREATE DATABASE football_stats;"
```

### Slow data loading
This is normal - rate limiting is intentional to respect data sources. The soccerdata library caches data, so subsequent runs will be faster.

## Tips

1. **Start Small**: Test with one league/season before loading everything
2. **Check Logs**: All operations are logged to `logs/football_db.log`
3. **Use Cache**: The soccerdata library caches data locally - don't use `--force-refresh` unless needed
4. **Be Patient**: Initial historical load takes time due to rate limiting
5. **Backup**: Regular database backups are recommended

## Getting Help

- Check main README.md for detailed documentation
- Review log files in `logs/` directory
- Verify configuration in `config/db_config.yaml`
- Ensure PostgreSQL is running: `sudo systemctl status postgresql`

## What's Next?

After setup, you can:
- Build analytics queries
- Create views for common reports
- Set up a dashboard (Grafana, Metabase)
- Export data for machine learning
- Integrate with other applications

Enjoy your football statistics database!
