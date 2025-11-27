"""
Historical data loader for football statistics database.

This script orchestrates loading all historical data from
the 2020-2021 season onwards for all configured leagues.
"""

import sys
from pathlib import Path
from typing import List, Optional
from datetime import datetime, timedelta
import time

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from football_db.scripts.db_utils import get_db_connection, DatabaseConfig
from football_db.scripts.logging_utils import setup_logging
from football_db.scripts.load_matches import MatchDataLoader
from football_db.scripts.load_team_stats import TeamStatsLoader
from football_db.scripts.load_player_stats import PlayerStatsLoader
from football_db.scripts.load_standings import StandingsLoader


class HistoricalDataLoader:
    """
    Orchestrates loading of all historical football data.

    Loads data for configured leagues and seasons:
    - Match fixtures and results
    - Team season statistics
    - Player season statistics
    - League standings
    """

    def __init__(self, db_connection, logger, config: DatabaseConfig):
        """
        Initialize historical data loader.

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

    def load_all_historical_data(
        self,
        leagues: Optional[List[str]] = None,
        start_season: Optional[str] = None,
        end_season: Optional[str] = None,
        force_refresh: bool = False,
        skip_matches: bool = False,
        skip_team_stats: bool = False,
        skip_player_stats: bool = False,
        skip_standings: bool = False
    ) -> None:
        """
        Load all historical data for specified leagues and seasons.

        Args:
            leagues: List of league codes (default: from config)
            start_season: Starting season (default: from config)
            end_season: Ending season (default: from config)
            force_refresh: If True, refresh cached data
            skip_matches: If True, skip loading matches
            skip_team_stats: If True, skip loading team stats
            skip_player_stats: If True, skip loading player stats
            skip_standings: If True, skip loading standings
        """
        # Get leagues from config if not specified
        if leagues is None:
            leagues = [
                league['code']
                for league in self.config.config.get('leagues', [])
            ]

        # Get seasons from config if not specified
        if start_season is None:
            start_season = self.config.config.get('seasons', {}).get('start_season', '2020-2021')
        if end_season is None:
            end_season = self.config.config.get('seasons', {}).get('end_season', '2024-2025')

        # Generate list of seasons
        seasons = self._generate_seasons(start_season, end_season)

        self.logger.info("=" * 80)
        self.logger.info("HISTORICAL DATA LOAD STARTED")
        self.logger.info("=" * 80)
        self.logger.info(f"Leagues: {', '.join(leagues)}")
        self.logger.info(f"Seasons: {start_season} to {end_season} ({len(seasons)} seasons)")
        self.logger.info(f"Force refresh: {force_refresh}")
        self.logger.info("=" * 80)

        start_time = datetime.now()
        total_stats = {
            'matches': {'inserted': 0, 'updated': 0, 'failed': 0},
            'team_stats': {'inserted': 0, 'updated': 0, 'failed': 0},
            'player_stats': {'inserted': 0, 'updated': 0, 'failed': 0},
            'standings': {'inserted': 0, 'updated': 0, 'failed': 0}
        }

        # Process each league and season
        for league_code in leagues:
            self.logger.info(f"\n{'=' * 80}")
            self.logger.info(f"Processing league: {league_code}")
            self.logger.info(f"{'=' * 80}")

            for season in seasons:
                self.logger.info(f"\n{'-' * 80}")
                self.logger.info(f"Season: {season}")
                self.logger.info(f"{'-' * 80}")

                try:
                    # Load matches
                    if not skip_matches:
                        self.logger.info("\n1. Loading matches...")
                        stats = self.match_loader.load_matches_for_league_season(
                            league_code=league_code,
                            season=season,
                            force_refresh=force_refresh
                        )
                        for key in stats:
                            total_stats['matches'][key] += stats[key]
                        self.logger.info(f"   Matches: {stats}")
                        time.sleep(2)  # Rate limiting

                    # Load team stats
                    if not skip_team_stats:
                        self.logger.info("\n2. Loading team statistics...")
                        stats = self.team_stats_loader.load_team_stats_for_league_season(
                            league_code=league_code,
                            season=season,
                            force_refresh=force_refresh
                        )
                        for key in stats:
                            total_stats['team_stats'][key] += stats[key]
                        self.logger.info(f"   Team stats: {stats}")
                        time.sleep(2)  # Rate limiting

                    # Load player stats
                    if not skip_player_stats:
                        self.logger.info("\n3. Loading player statistics...")
                        stats = self.player_stats_loader.load_player_stats_for_league_season(
                            league_code=league_code,
                            season=season,
                            force_refresh=force_refresh
                        )
                        for key in stats:
                            total_stats['player_stats'][key] += stats[key]
                        self.logger.info(f"   Player stats: {stats}")
                        time.sleep(2)  # Rate limiting

                    # Load standings
                    if not skip_standings:
                        self.logger.info("\n4. Loading league standings...")
                        stats = self.standings_loader.load_standings_for_league_season(
                            league_code=league_code,
                            season=season,
                            force_refresh=force_refresh
                        )
                        for key in stats:
                            total_stats['standings'][key] += stats[key]
                        self.logger.info(f"   Standings: {stats}")
                        time.sleep(2)  # Rate limiting

                    self.logger.info(f"\n✓ Completed {league_code} {season}")

                except Exception as e:
                    self.logger.error(
                        f"Error processing {league_code} {season}: {e}",
                        exc_info=True
                    )
                    self.logger.info("Continuing with next season...")
                    continue

        # Print summary
        end_time = datetime.now()
        duration = end_time - start_time

        self.logger.info("\n" + "=" * 80)
        self.logger.info("HISTORICAL DATA LOAD COMPLETED")
        self.logger.info("=" * 80)
        self.logger.info(f"Duration: {duration}")
        self.logger.info("\nSummary:")
        self.logger.info(f"  Matches:       {total_stats['matches']}")
        self.logger.info(f"  Team Stats:    {total_stats['team_stats']}")
        self.logger.info(f"  Player Stats:  {total_stats['player_stats']}")
        self.logger.info(f"  Standings:     {total_stats['standings']}")
        self.logger.info("=" * 80)

    def _generate_seasons(self, start_season: str, end_season: str) -> List[str]:
        """
        Generate list of seasons between start and end.

        Args:
            start_season: Starting season (e.g., '2020-2021')
            end_season: Ending season (e.g., '2024-2025')

        Returns:
            List of season strings
        """
        seasons = []
        start_year = int(start_season.split('-')[0])
        end_year = int(end_season.split('-')[0])

        for year in range(start_year, end_year + 1):
            seasons.append(f"{year}-{year + 1}")

        return seasons


def main():
    """Main function for command-line usage."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Load historical football data into database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Load all data for all configured leagues and seasons
  python load_historical_data.py

  # Load data for specific league
  python load_historical_data.py --leagues "ENG-Premier League"

  # Load data for specific season range
  python load_historical_data.py --start-season 2022-2023 --end-season 2023-2024

  # Load only matches and standings (skip stats)
  python load_historical_data.py --skip-team-stats --skip-player-stats

  # Force refresh all cached data
  python load_historical_data.py --force-refresh
        """
    )

    parser.add_argument(
        '--leagues',
        nargs='+',
        help='League codes to process (default: all from config)'
    )
    parser.add_argument(
        '--start-season',
        help='Starting season (e.g., 2020-2021)'
    )
    parser.add_argument(
        '--end-season',
        help='Ending season (e.g., 2024-2025)'
    )
    parser.add_argument(
        '--force-refresh',
        action='store_true',
        help='Force refresh of cached data'
    )
    parser.add_argument(
        '--skip-matches',
        action='store_true',
        help='Skip loading match data'
    )
    parser.add_argument(
        '--skip-team-stats',
        action='store_true',
        help='Skip loading team statistics'
    )
    parser.add_argument(
        '--skip-player-stats',
        action='store_true',
        help='Skip loading player statistics'
    )
    parser.add_argument(
        '--skip-standings',
        action='store_true',
        help='Skip loading league standings'
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

        # Create loader
        loader = HistoricalDataLoader(db, logger, config)

        # Load historical data
        loader.load_all_historical_data(
            leagues=args.leagues,
            start_season=args.start_season,
            end_season=args.end_season,
            force_refresh=args.force_refresh,
            skip_matches=args.skip_matches,
            skip_team_stats=args.skip_team_stats,
            skip_player_stats=args.skip_player_stats,
            skip_standings=args.skip_standings
        )

        logger.info("\n✓ Historical data load completed successfully!")

    except KeyboardInterrupt:
        logger.warning("\n\nLoad interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n✗ Historical data load failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
