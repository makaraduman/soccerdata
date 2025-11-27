"""
League standings loader for football statistics database.

This script loads league table/standings from FBref
into the PostgreSQL database.
"""

import sys
from pathlib import Path
from typing import Dict
from datetime import datetime, date
import pandas as pd

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import soccerdata as sd
from football_db.scripts.db_utils import get_db_connection, DatabaseConfig
from football_db.scripts.logging_utils import setup_logging, DataLoadLogger


class StandingsLoader:
    """Loads league standings from FBref into the database."""

    def __init__(self, db_connection, logger):
        """
        Initialize standings loader.

        Args:
            db_connection: DatabaseConnection instance
            logger: Logger instance
        """
        self.db = db_connection
        self.logger = logger
        self.load_logger = DataLoadLogger(db_connection, logger)
        self.config = DatabaseConfig()

    def load_standings_for_league_season(
        self,
        league_code: str,
        season: str,
        standing_date: date = None,
        force_refresh: bool = False
    ) -> Dict[str, int]:
        """
        Load league standings for a specific league and season.

        Args:
            league_code: League code (e.g., 'ENG-Premier League')
            season: Season name (e.g., '2020-2021')
            standing_date: Date of standings snapshot (default: today)
            force_refresh: If True, refresh cached data

        Returns:
            Dictionary with counts of inserted/updated records
        """
        if standing_date is None:
            standing_date = date.today()

        self.logger.info(f"Loading standings for {league_code} {season}")

        # Get league_season_id
        league_season_id = self.db.get_league_season_id(league_code, season)
        if not league_season_id:
            self.logger.error(f"League season not found: {league_code} {season}")
            return {'inserted': 0, 'updated': 0, 'failed': 0}

        # Start load logging
        load_id = self.load_logger.start_load(
            source_name='FBref',
            load_type='standings_load',
            target_table='league_standings',
            league_season_id=league_season_id
        )

        try:
            # Initialize FBref scraper
            fbref = sd.FBref(
                leagues=league_code,
                seasons=season,
                no_cache=force_refresh
            )

            # Fetch standings data
            self.logger.info("Fetching league standings from FBref...")
            standings_df = fbref.read_league_table()

            if standings_df.empty:
                self.logger.warning(f"No standings found for {league_code} {season}")
                self.load_logger.complete_load(status='completed')
                return {'inserted': 0, 'updated': 0, 'failed': 0}

            self.logger.info(f"Found standings for {len(standings_df)} teams")

            # Process and insert standings
            stats = self._process_standings(
                standings_df,
                league_season_id,
                standing_date
            )

            # Update load progress
            self.load_logger.update_progress(
                records_processed=len(standings_df),
                records_inserted=stats['inserted'],
                records_updated=stats['updated'],
                records_failed=stats['failed']
            )

            # Complete load
            self.load_logger.complete_load(status='completed')

            return stats

        except Exception as e:
            self.load_logger.log_error(
                e,
                f"Loading standings for {league_code} {season}"
            )
            raise

    def _process_standings(
        self,
        standings_df: pd.DataFrame,
        league_season_id: int,
        standing_date: date
    ) -> Dict[str, int]:
        """
        Process and insert league standings.

        Args:
            standings_df: DataFrame with standings
            league_season_id: League season ID
            standing_date: Date of standings snapshot

        Returns:
            Statistics dictionary
        """
        inserted = 0
        updated = 0
        failed = 0

        # Reset index if multi-index
        if isinstance(standings_df.index, pd.MultiIndex):
            standings_df = standings_df.reset_index()

        # Get team name column
        team_col = None
        for col in ['team', 'Team', 'Squad']:
            if col in standings_df.columns:
                team_col = col
                break
        if team_col is None and 'team' in standings_df.index.names:
            standings_df = standings_df.reset_index()
            team_col = 'team'

        if team_col is None:
            self.logger.error("Could not find team column in standings dataframe")
            return {'inserted': 0, 'updated': 0, 'failed': len(standings_df)}

        for idx, row in standings_df.iterrows():
            try:
                team_name = row[team_col]
                if pd.isna(team_name):
                    failed += 1
                    continue

                # Get or create team
                team_id = self.db.get_or_create_team(str(team_name))

                # Extract standings data
                position = idx + 1  # Use row index as position
                if 'Rk' in row.index:
                    position = int(row['Rk']) if pd.notna(row['Rk']) else position

                # Helper function to get value
                def get_val(col_names, default=0):
                    for col in col_names if isinstance(col_names, list) else [col_names]:
                        if col in row.index and pd.notna(row[col]):
                            try:
                                return int(row[col])
                            except:
                                return default
                    return default

                matches_played = get_val(['MP', 'Matches', 'Pld'])
                wins = get_val(['W', 'Wins'])
                draws = get_val(['D', 'Draws'])
                losses = get_val(['L', 'Losses'])
                goals_for = get_val(['GF', 'Goals For', 'F'])
                goals_against = get_val(['GA', 'Goals Against', 'A'])
                goal_difference = get_val(['GD', 'Goal Difference'], goals_for - goals_against)
                points = get_val(['Pts', 'Points'])

                # Home/Away records (if available)
                home_wins = get_val(['Home W'], None)
                home_draws = get_val(['Home D'], None)
                home_losses = get_val(['Home L'], None)
                away_wins = get_val(['Away W'], None)
                away_draws = get_val(['Away D'], None)
                away_losses = get_val(['Away L'], None)

                # Prepare upsert data
                upsert_data = [(
                    league_season_id,
                    team_id,
                    position,
                    matches_played,
                    wins,
                    draws,
                    losses,
                    goals_for,
                    goals_against,
                    goal_difference,
                    points,
                    home_wins,
                    home_draws,
                    home_losses,
                    away_wins,
                    away_draws,
                    away_losses,
                    standing_date
                )]

                rows_affected = self.db.upsert(
                    table='league_standings',
                    columns=[
                        'league_season_id', 'team_id', 'position',
                        'matches_played', 'wins', 'draws', 'losses',
                        'goals_for', 'goals_against', 'goal_difference',
                        'points', 'home_wins', 'home_draws', 'home_losses',
                        'away_wins', 'away_draws', 'away_losses',
                        'standing_date'
                    ],
                    data=upsert_data,
                    conflict_columns=['league_season_id', 'team_id', 'standing_date']
                )

                if rows_affected > 0:
                    updated += 1
                else:
                    inserted += 1

            except Exception as e:
                self.logger.error(f"Error processing standing at row {idx}: {e}")
                failed += 1
                continue

        self.logger.info(
            f"Standings processing complete: {inserted} inserted, "
            f"{updated} updated, {failed} failed"
        )

        return {
            'inserted': inserted,
            'updated': updated,
            'failed': failed
        }


def main():
    """Main function for command-line usage."""
    import argparse

    parser = argparse.ArgumentParser(description='Load league standings into database')
    parser.add_argument(
        '--league',
        required=True,
        help='League code (e.g., ENG-Premier League)'
    )
    parser.add_argument(
        '--season',
        required=True,
        help='Season (e.g., 2020-2021)'
    )
    parser.add_argument(
        '--date',
        help='Standing date (YYYY-MM-DD, default: today)'
    )
    parser.add_argument(
        '--force-refresh',
        action='store_true',
        help='Force refresh of cached data'
    )
    parser.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level'
    )

    args = parser.parse_args()

    # Parse date
    standing_date = None
    if args.date:
        try:
            standing_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        except ValueError:
            print(f"Invalid date format: {args.date}. Use YYYY-MM-DD")
            sys.exit(1)

    # Setup logging
    logger = setup_logging(log_level=args.log_level)

    try:
        # Get database connection
        db = get_db_connection()

        # Create loader
        loader = StandingsLoader(db, logger)

        # Load standings
        stats = loader.load_standings_for_league_season(
            league_code=args.league,
            season=args.season,
            standing_date=standing_date,
            force_refresh=args.force_refresh
        )

        logger.info(f"Standings load complete: {stats}")

    except Exception as e:
        logger.error(f"Standings load failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
