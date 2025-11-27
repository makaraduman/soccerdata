"""
Daily update script for football statistics database.

This script updates the database with the latest match results,
statistics, and standings for the current season.
"""

import sys
from pathlib import Path
from typing import List, Optional
from datetime import datetime, date, timedelta

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from football_db.scripts.db_utils import get_db_connection, DatabaseConfig
from football_db.scripts.logging_utils import setup_logging
from football_db.scripts.load_matches import MatchDataLoader
from football_db.scripts.load_team_stats import TeamStatsLoader
from football_db.scripts.load_player_stats import PlayerStatsLoader
from football_db.scripts.load_standings import StandingsLoader


class DailyUpdater:
    """
    Daily update orchestrator for football statistics.

    Updates:
    - Match results for recently completed matches
    - Upcoming fixtures
    - Current season team and player statistics
    - Current league standings
    """

    def __init__(self, db_connection, logger, config: DatabaseConfig):
        """
        Initialize daily updater.

        Args:
            db_connection: DatabaseConnection instance
            logger: Logger instance
            config: DatabaseConfig instance
        """
        self.db = db_connection
        self.logger = logger
        self.config = config

        # Initialize individual loaders
        self.match_loader = MatchDataLoader(db_connection, logger)
        self.team_stats_loader = TeamStatsLoader(db_connection, logger)
        self.player_stats_loader = PlayerStatsLoader(db_connection, logger)
        self.standings_loader = StandingsLoader(db_connection, logger)

    def run_daily_update(
        self,
        leagues: Optional[List[str]] = None,
        current_season: Optional[str] = None,
        update_matches: bool = True,
        update_stats: bool = True,
        update_standings: bool = True
    ) -> None:
        """
        Run daily update for all configured leagues.

        Args:
            leagues: List of league codes (default: from config)
            current_season: Current season (default: auto-detect)
            update_matches: If True, update match data
            update_stats: If True, update team and player stats
            update_standings: If True, update league standings
        """
        # Get leagues from config if not specified
        if leagues is None:
            leagues = [
                league['code']
                for league in self.config.config.get('leagues', [])
            ]

        # Determine current season if not specified
        if current_season is None:
            current_season = self._get_current_season()

        self.logger.info("=" * 80)
        self.logger.info("DAILY UPDATE STARTED")
        self.logger.info("=" * 80)
        self.logger.info(f"Date: {date.today()}")
        self.logger.info(f"Leagues: {', '.join(leagues)}")
        self.logger.info(f"Season: {current_season}")
        self.logger.info("=" * 80)

        start_time = datetime.now()
        total_stats = {
            'matches': {'inserted': 0, 'updated': 0, 'failed': 0},
            'team_stats': {'inserted': 0, 'updated': 0, 'failed': 0},
            'player_stats': {'inserted': 0, 'updated': 0, 'failed': 0},
            'standings': {'inserted': 0, 'updated': 0, 'failed': 0}
        }

        # Process each league
        for league_code in leagues:
            self.logger.info(f"\n{'=' * 80}")
            self.logger.info(f"Updating: {league_code}")
            self.logger.info(f"{'=' * 80}")

            try:
                # Update matches (always refresh to get latest results)
                if update_matches:
                    self.logger.info("\n1. Updating matches...")
                    stats = self.match_loader.load_matches_for_league_season(
                        league_code=league_code,
                        season=current_season,
                        force_refresh=True  # Always refresh for daily updates
                    )
                    for key in stats:
                        total_stats['matches'][key] += stats[key]
                    self.logger.info(f"   Matches updated: {stats}")

                # Update team stats
                if update_stats:
                    self.logger.info("\n2. Updating team statistics...")
                    stats = self.team_stats_loader.load_team_stats_for_league_season(
                        league_code=league_code,
                        season=current_season,
                        force_refresh=True
                    )
                    for key in stats:
                        total_stats['team_stats'][key] += stats[key]
                    self.logger.info(f"   Team stats updated: {stats}")

                # Update player stats
                if update_stats:
                    self.logger.info("\n3. Updating player statistics...")
                    stats = self.player_stats_loader.load_player_stats_for_league_season(
                        league_code=league_code,
                        season=current_season,
                        force_refresh=True
                    )
                    for key in stats:
                        total_stats['player_stats'][key] += stats[key]
                    self.logger.info(f"   Player stats updated: {stats}")

                # Update standings
                if update_standings:
                    self.logger.info("\n4. Updating league standings...")
                    stats = self.standings_loader.load_standings_for_league_season(
                        league_code=league_code,
                        season=current_season,
                        standing_date=date.today(),
                        force_refresh=True
                    )
                    for key in stats:
                        total_stats['standings'][key] += stats[key]
                    self.logger.info(f"   Standings updated: {stats}")

                self.logger.info(f"\n✓ Completed {league_code}")

            except Exception as e:
                self.logger.error(
                    f"Error updating {league_code}: {e}",
                    exc_info=True
                )
                self.logger.info("Continuing with next league...")
                continue

        # Print summary
        end_time = datetime.now()
        duration = end_time - start_time

        self.logger.info("\n" + "=" * 80)
        self.logger.info("DAILY UPDATE COMPLETED")
        self.logger.info("=" * 80)
        self.logger.info(f"Duration: {duration}")
        self.logger.info("\nSummary:")
        self.logger.info(f"  Matches:       {total_stats['matches']}")
        self.logger.info(f"  Team Stats:    {total_stats['team_stats']}")
        self.logger.info(f"  Player Stats:  {total_stats['player_stats']}")
        self.logger.info(f"  Standings:     {total_stats['standings']}")
        self.logger.info("=" * 80)

    def _get_current_season(self) -> str:
        """
        Determine current season based on current date.

        Returns:
            Season string (e.g., '2024-2025')
        """
        today = date.today()
        year = today.year

        # European football season typically runs from August to May
        # If we're between January and July, the season started last year
        if today.month < 8:
            start_year = year - 1
        else:
            start_year = year

        return f"{start_year}-{start_year + 1}"


def main():
    """Main function for command-line usage."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Run daily update for football statistics database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run daily update for all leagues
  python daily_update.py

  # Update specific leagues only
  python daily_update.py --leagues "ENG-Premier League" "ESP-La Liga"

  # Update specific season
  python daily_update.py --season 2024-2025

  # Update only matches and standings (skip stats)
  python daily_update.py --no-stats

  # Update only matches
  python daily_update.py --no-stats --no-standings
        """
    )

    parser.add_argument(
        '--leagues',
        nargs='+',
        help='League codes to update (default: all from config)'
    )
    parser.add_argument(
        '--season',
        help='Season to update (default: auto-detect current season)'
    )
    parser.add_argument(
        '--no-matches',
        action='store_true',
        help='Skip updating match data'
    )
    parser.add_argument(
        '--no-stats',
        action='store_true',
        help='Skip updating team and player statistics'
    )
    parser.add_argument(
        '--no-standings',
        action='store_true',
        help='Skip updating league standings'
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
        # Get database connection and config
        db = get_db_connection()
        config = DatabaseConfig()

        # Create updater
        updater = DailyUpdater(db, logger, config)

        # Run daily update
        updater.run_daily_update(
            leagues=args.leagues,
            current_season=args.season,
            update_matches=not args.no_matches,
            update_stats=not args.no_stats,
            update_standings=not args.no_standings
        )

        logger.info("\n✓ Daily update completed successfully!")

    except KeyboardInterrupt:
        logger.warning("\n\nUpdate interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n✗ Daily update failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
