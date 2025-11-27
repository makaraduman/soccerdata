"""
Match data loader for football statistics database.

This script loads match fixtures and results from FBref
into the PostgreSQL database.
"""

import sys
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime
import pandas as pd
import time

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import soccerdata as sd
from football_db.scripts.db_utils import get_db_connection, DatabaseConfig
from football_db.scripts.logging_utils import setup_logging, DataLoadLogger


class MatchDataLoader:
    """Loads match data from FBref into the database."""

    def __init__(self, db_connection, logger):
        """
        Initialize match data loader.

        Args:
            db_connection: DatabaseConnection instance
            logger: Logger instance
        """
        self.db = db_connection
        self.logger = logger
        self.load_logger = DataLoadLogger(db_connection, logger)
        self.config = DatabaseConfig()

    def load_matches_for_league_season(
        self,
        league_code: str,
        season: str,
        force_refresh: bool = False
    ) -> Dict[str, int]:
        """
        Load matches for a specific league and season.

        Args:
            league_code: League code (e.g., 'ENG-Premier League')
            season: Season name (e.g., '2020-2021')
            force_refresh: If True, refresh cached data

        Returns:
            Dictionary with counts of inserted/updated records
        """
        self.logger.info(f"Loading matches for {league_code} {season}")

        # Get league_season_id
        league_season_id = self.db.get_league_season_id(league_code, season)
        if not league_season_id:
            self.logger.error(f"League season not found: {league_code} {season}")
            return {'inserted': 0, 'updated': 0, 'failed': 0}

        # Start load logging
        load_id = self.load_logger.start_load(
            source_name='FBref',
            load_type='matches_load',
            target_table='matches',
            league_season_id=league_season_id
        )

        try:
            # Initialize FBref scraper
            fbref = sd.FBref(
                leagues=league_code,
                seasons=season,
                no_cache=force_refresh
            )

            # Fetch schedule data
            self.logger.info("Fetching match schedule from FBref...")
            schedule_df = fbref.read_schedule()

            if schedule_df.empty:
                self.logger.warning(f"No matches found for {league_code} {season}")
                self.load_logger.complete_load(status='completed')
                return {'inserted': 0, 'updated': 0, 'failed': 0}

            self.logger.info(f"Found {len(schedule_df)} matches")

            # Process and insert matches
            stats = self._process_matches(schedule_df, league_season_id)

            # Update load progress
            self.load_logger.update_progress(
                records_processed=len(schedule_df),
                records_inserted=stats['inserted'],
                records_updated=stats['updated'],
                records_failed=stats['failed']
            )

            # Complete load
            self.load_logger.complete_load(status='completed')

            return stats

        except Exception as e:
            self.load_logger.log_error(e, f"Loading matches for {league_code} {season}")
            raise

    def _process_matches(
        self,
        schedule_df: pd.DataFrame,
        league_season_id: int
    ) -> Dict[str, int]:
        """
        Process and insert match data.

        Args:
            schedule_df: DataFrame with match schedule
            league_season_id: League season ID

        Returns:
            Statistics dictionary
        """
        inserted = 0
        updated = 0
        failed = 0

        # Reset index if multi-index
        if isinstance(schedule_df.index, pd.MultiIndex):
            schedule_df = schedule_df.reset_index()

        for idx, row in schedule_df.iterrows():
            try:
                # Extract match data
                home_team = row.get('home_team', row.get('Home'))
                away_team = row.get('away_team', row.get('Away'))
                match_date = row.get('date', row.get('Date'))
                home_score = row.get('home_score', row.get('Score'))
                away_score = row.get('away_score', row.get('Score'))

                if not home_team or not away_team:
                    self.logger.warning(f"Skipping row {idx}: missing team names")
                    failed += 1
                    continue

                # Get or create team IDs
                home_team_id = self.db.get_or_create_team(str(home_team))
                away_team_id = self.db.get_or_create_team(str(away_team))

                # Parse match date
                if pd.isna(match_date):
                    self.logger.warning(f"Skipping match: missing date")
                    failed += 1
                    continue

                match_datetime = pd.to_datetime(match_date)

                # Parse scores
                if pd.notna(home_score) and isinstance(home_score, str) and '–' in home_score:
                    # Score format "2–1"
                    scores = home_score.split('–')
                    home_score_val = int(scores[0]) if len(scores) > 0 else None
                    away_score_val = int(scores[1]) if len(scores) > 1 else None
                elif pd.notna(home_score) and pd.notna(away_score):
                    home_score_val = int(home_score) if not pd.isna(home_score) else None
                    away_score_val = int(away_score) if not pd.isna(away_score) else None
                else:
                    home_score_val = None
                    away_score_val = None

                # Determine match status
                if home_score_val is not None:
                    match_status = 'completed'
                else:
                    match_status = 'scheduled'

                # Extract additional fields
                matchweek = row.get('matchweek', row.get('Week', None))
                if pd.notna(matchweek):
                    matchweek = int(matchweek)
                else:
                    matchweek = None

                venue = row.get('venue', row.get('Venue', None))
                if pd.notna(venue):
                    venue = str(venue)[:100]
                else:
                    venue = None

                referee = row.get('referee', row.get('Referee', None))
                if pd.notna(referee):
                    referee = str(referee)[:100]
                else:
                    referee = None

                attendance = row.get('attendance', row.get('Attendance', None))
                if pd.notna(attendance):
                    try:
                        attendance = int(str(attendance).replace(',', ''))
                    except:
                        attendance = None
                else:
                    attendance = None

                # Create external match ID for deduplication
                external_match_id = f"fbref_{league_season_id}_{home_team}_{away_team}_{match_datetime.date()}"

                # Upsert match
                match_data = [(
                    league_season_id,
                    home_team_id,
                    away_team_id,
                    match_datetime,
                    matchweek,
                    home_score_val,
                    away_score_val,
                    None,  # home_halftime_score
                    None,  # away_halftime_score
                    attendance,
                    venue,
                    referee,
                    match_status,
                    external_match_id
                )]

                rows_affected = self.db.upsert(
                    table='matches',
                    columns=[
                        'league_season_id', 'home_team_id', 'away_team_id',
                        'match_date', 'matchweek', 'home_score', 'away_score',
                        'home_halftime_score', 'away_halftime_score',
                        'attendance', 'venue', 'referee', 'match_status',
                        'external_match_id'
                    ],
                    data=match_data,
                    conflict_columns=['league_season_id', 'home_team_id', 'away_team_id', 'match_date'],
                    update_columns=[
                        'matchweek', 'home_score', 'away_score',
                        'attendance', 'venue', 'referee', 'match_status'
                    ]
                )

                if rows_affected > 0:
                    # Check if it was an insert or update
                    # (this is approximate - could be improved with RETURNING clause)
                    if match_status == 'completed':
                        updated += 1
                    else:
                        inserted += 1

            except Exception as e:
                self.logger.error(f"Error processing match at row {idx}: {e}")
                failed += 1
                continue

        self.logger.info(
            f"Match processing complete: {inserted} inserted, "
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

    parser = argparse.ArgumentParser(description='Load match data into database')
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

    # Setup logging
    logger = setup_logging(log_level=args.log_level)

    try:
        # Get database connection
        db = get_db_connection()

        # Create loader
        loader = MatchDataLoader(db, logger)

        # Load matches
        stats = loader.load_matches_for_league_season(
            league_code=args.league,
            season=args.season,
            force_refresh=args.force_refresh
        )

        logger.info(f"Match load complete: {stats}")

    except Exception as e:
        logger.error(f"Match load failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
