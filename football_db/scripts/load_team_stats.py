"""
Team statistics loader for football statistics database.

This script loads team season statistics from FBref
into the PostgreSQL database.
"""

import sys
from pathlib import Path
from typing import List, Dict, Optional
import pandas as pd
import json

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import soccerdata as sd
from football_db.scripts.db_utils import get_db_connection, DatabaseConfig
from football_db.scripts.logging_utils import setup_logging, DataLoadLogger


class TeamStatsLoader:
    """Loads team statistics from FBref into the database."""

    STAT_TYPES = [
        'standard',
        'shooting',
        'passing',
        'passing_types',
        'defense',
        'possession',
        'misc'
    ]

    def __init__(self, db_connection, logger):
        """
        Initialize team stats loader.

        Args:
            db_connection: DatabaseConnection instance
            logger: Logger instance
        """
        self.db = db_connection
        self.logger = logger
        self.load_logger = DataLoadLogger(db_connection, logger)
        self.config = DatabaseConfig()

    def load_team_stats_for_league_season(
        self,
        league_code: str,
        season: str,
        stat_types: Optional[List[str]] = None,
        force_refresh: bool = False
    ) -> Dict[str, int]:
        """
        Load team statistics for a specific league and season.

        Args:
            league_code: League code (e.g., 'ENG-Premier League')
            season: Season name (e.g., '2020-2021')
            stat_types: List of stat types to load (default: all)
            force_refresh: If True, refresh cached data

        Returns:
            Dictionary with counts of inserted/updated records
        """
        if stat_types is None:
            stat_types = self.STAT_TYPES

        self.logger.info(f"Loading team stats for {league_code} {season}")

        # Get league_season_id
        league_season_id = self.db.get_league_season_id(league_code, season)
        if not league_season_id:
            self.logger.error(f"League season not found: {league_code} {season}")
            return {'inserted': 0, 'updated': 0, 'failed': 0}

        total_stats = {'inserted': 0, 'updated': 0, 'failed': 0}

        try:
            # Initialize FBref scraper
            fbref = sd.FBref(
                leagues=league_code,
                seasons=season,
                no_cache=force_refresh
            )

            # Load each stat type
            for stat_type in stat_types:
                self.logger.info(f"Loading {stat_type} stats...")

                # Start load logging
                load_id = self.load_logger.start_load(
                    source_name='FBref',
                    load_type=f'team_stats_{stat_type}',
                    target_table='team_season_stats',
                    league_season_id=league_season_id
                )

                try:
                    # Fetch team stats
                    stats_df = fbref.read_team_season_stats(stat_type=stat_type)

                    if stats_df.empty:
                        self.logger.warning(f"No {stat_type} stats found")
                        self.load_logger.complete_load(status='completed')
                        continue

                    self.logger.info(f"Found {len(stats_df)} team records")

                    # Process and insert stats
                    stats = self._process_team_stats(
                        stats_df,
                        league_season_id,
                        stat_type
                    )

                    # Update totals
                    for key in total_stats:
                        total_stats[key] += stats[key]

                    # Update load progress
                    self.load_logger.update_progress(
                        records_processed=len(stats_df),
                        records_inserted=stats['inserted'],
                        records_updated=stats['updated'],
                        records_failed=stats['failed']
                    )

                    # Complete load
                    self.load_logger.complete_load(status='completed')

                except Exception as e:
                    self.load_logger.log_error(
                        e,
                        f"Loading {stat_type} stats for {league_code} {season}"
                    )
                    continue

            return total_stats

        except Exception as e:
            self.logger.error(f"Team stats load failed: {e}", exc_info=True)
            raise

    def _process_team_stats(
        self,
        stats_df: pd.DataFrame,
        league_season_id: int,
        stat_type: str
    ) -> Dict[str, int]:
        """
        Process and insert team statistics.

        Args:
            stats_df: DataFrame with team stats
            league_season_id: League season ID
            stat_type: Type of statistics

        Returns:
            Statistics dictionary
        """
        inserted = 0
        updated = 0
        failed = 0

        # Reset index if multi-index
        if isinstance(stats_df.index, pd.MultiIndex):
            stats_df = stats_df.reset_index()

        # Get team name column (might be in index or column)
        team_col = None
        for col in ['team', 'Team', 'Squad']:
            if col in stats_df.columns:
                team_col = col
                break
        if team_col is None and 'team' in stats_df.index.names:
            stats_df = stats_df.reset_index()
            team_col = 'team'

        if team_col is None:
            self.logger.error("Could not find team column in stats dataframe")
            return {'inserted': 0, 'updated': 0, 'failed': len(stats_df)}

        for idx, row in stats_df.iterrows():
            try:
                team_name = row[team_col]
                if pd.isna(team_name):
                    failed += 1
                    continue

                # Get or create team
                team_id = self.db.get_or_create_team(str(team_name))

                # Extract stats based on type
                stats_data = self._extract_stats(row, stat_type)

                # Prepare upsert data
                upsert_data = [(
                    team_id,
                    league_season_id,
                    stat_type,
                    stats_data.get('matches_played'),
                    stats_data.get('wins'),
                    stats_data.get('draws'),
                    stats_data.get('losses'),
                    stats_data.get('goals_for'),
                    stats_data.get('goals_against'),
                    stats_data.get('possession_pct'),
                    stats_data.get('passes_completed'),
                    stats_data.get('passes_attempted'),
                    stats_data.get('pass_completion_pct'),
                    stats_data.get('shots'),
                    stats_data.get('shots_on_target'),
                    stats_data.get('shots_on_target_pct'),
                    stats_data.get('tackles'),
                    stats_data.get('tackles_won'),
                    stats_data.get('interceptions'),
                    stats_data.get('blocks'),
                    stats_data.get('clearances'),
                    stats_data.get('yellow_cards'),
                    stats_data.get('red_cards'),
                    stats_data.get('fouls_committed'),
                    stats_data.get('fouls_drawn'),
                    json.dumps(stats_data.get('additional_stats', {}))
                )]

                rows_affected = self.db.upsert(
                    table='team_season_stats',
                    columns=[
                        'team_id', 'league_season_id', 'stat_type',
                        'matches_played', 'wins', 'draws', 'losses',
                        'goals_for', 'goals_against',
                        'possession_pct', 'passes_completed', 'passes_attempted',
                        'pass_completion_pct', 'shots', 'shots_on_target',
                        'shots_on_target_pct', 'tackles', 'tackles_won',
                        'interceptions', 'blocks', 'clearances',
                        'yellow_cards', 'red_cards', 'fouls_committed',
                        'fouls_drawn', 'additional_stats'
                    ],
                    data=upsert_data,
                    conflict_columns=['team_id', 'league_season_id', 'stat_type']
                )

                if rows_affected > 0:
                    updated += 1
                else:
                    inserted += 1

            except Exception as e:
                self.logger.error(f"Error processing team stats at row {idx}: {e}")
                failed += 1
                continue

        self.logger.info(
            f"Team stats processing complete: {inserted} inserted, "
            f"{updated} updated, {failed} failed"
        )

        return {
            'inserted': inserted,
            'updated': updated,
            'failed': failed
        }

    def _extract_stats(self, row: pd.Series, stat_type: str) -> Dict:
        """
        Extract statistics from row based on stat type.

        Args:
            row: DataFrame row
            stat_type: Type of statistics

        Returns:
            Dictionary of extracted stats
        """
        stats = {'additional_stats': {}}

        # Helper function to get value
        def get_val(col_names, convert_func=None):
            for col in col_names if isinstance(col_names, list) else [col_names]:
                if col in row.index and pd.notna(row[col]):
                    val = row[col]
                    if convert_func:
                        try:
                            return convert_func(val)
                        except:
                            return None
                    return val
            return None

        # Standard stats
        if stat_type == 'standard':
            stats['matches_played'] = get_val(['# Pl', 'MP', 'Matches'], int)
            stats['wins'] = get_val(['W', 'Wins'], int)
            stats['draws'] = get_val(['D', 'Draws'], int)
            stats['losses'] = get_val(['L', 'Losses'], int)
            stats['goals_for'] = get_val(['GF', 'Goals For'], int)
            stats['goals_against'] = get_val(['GA', 'Goals Against'], int)
            stats['yellow_cards'] = get_val(['CrdY', 'Yellow Cards'], int)
            stats['red_cards'] = get_val(['CrdR', 'Red Cards'], int)

        # Shooting stats
        elif stat_type == 'shooting':
            stats['shots'] = get_val(['Sh', 'Shots'], int)
            stats['shots_on_target'] = get_val(['SoT', 'Shots on Target'], int)
            stats['shots_on_target_pct'] = get_val(['SoT%'], float)
            stats['additional_stats']['goals_per_shot'] = get_val(['G/Sh'], float)
            stats['additional_stats']['goals_per_shot_on_target'] = get_val(['G/SoT'], float)

        # Passing stats
        elif stat_type == 'passing':
            stats['passes_completed'] = get_val(['Cmp', 'Passes Completed'], int)
            stats['passes_attempted'] = get_val(['Att', 'Passes Attempted'], int)
            stats['pass_completion_pct'] = get_val(['Cmp%', 'Pass Completion %'], float)
            stats['additional_stats']['progressive_passes'] = get_val(['PrgP'], int)
            stats['additional_stats']['key_passes'] = get_val(['KP'], int)

        # Defensive stats
        elif stat_type == 'defense':
            stats['tackles'] = get_val(['Tkl', 'Tackles'], int)
            stats['tackles_won'] = get_val(['TklW'], int)
            stats['interceptions'] = get_val(['Int', 'Interceptions'], int)
            stats['blocks'] = get_val(['Blocks'], int)
            stats['clearances'] = get_val(['Clr', 'Clearances'], int)

        # Possession stats
        elif stat_type == 'possession':
            stats['possession_pct'] = get_val(['Poss', 'Possession'], float)
            stats['additional_stats']['touches'] = get_val(['Touches'], int)
            stats['additional_stats']['progressive_carries'] = get_val(['PrgC'], int)

        # Misc stats
        elif stat_type == 'misc':
            stats['fouls_committed'] = get_val(['Fls', 'Fouls'], int)
            stats['fouls_drawn'] = get_val(['Fld', 'Fouls Drawn'], int)
            stats['additional_stats']['offsides'] = get_val(['Off', 'Offsides'], int)
            stats['additional_stats']['penalty_kicks'] = get_val(['PKwon'], int)

        # Store all other columns in additional_stats
        for col in row.index:
            if col not in ['team', 'Team', 'Squad'] and col not in stats:
                val = row[col]
                if pd.notna(val):
                    stats['additional_stats'][col] = val

        return stats


def main():
    """Main function for command-line usage."""
    import argparse

    parser = argparse.ArgumentParser(description='Load team statistics into database')
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
        '--stat-types',
        nargs='+',
        choices=TeamStatsLoader.STAT_TYPES,
        help='Stat types to load (default: all)'
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
        loader = TeamStatsLoader(db, logger)

        # Load team stats
        stats = loader.load_team_stats_for_league_season(
            league_code=args.league,
            season=args.season,
            stat_types=args.stat_types,
            force_refresh=args.force_refresh
        )

        logger.info(f"Team stats load complete: {stats}")

    except Exception as e:
        logger.error(f"Team stats load failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
