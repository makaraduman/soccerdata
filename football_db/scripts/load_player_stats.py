"""
Player statistics loader for football statistics database.

This script loads player season statistics from FBref
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


class PlayerStatsLoader:
    """Loads player statistics from FBref into the database."""

    STAT_TYPES = [
        'standard',
        'shooting',
        'passing',
        'defense',
        'possession',
        'goalkeeping'
    ]

    def __init__(self, db_connection, logger):
        """
        Initialize player stats loader.

        Args:
            db_connection: DatabaseConnection instance
            logger: Logger instance
        """
        self.db = db_connection
        self.logger = logger
        self.load_logger = DataLoadLogger(db_connection, logger)
        self.config = DatabaseConfig()

    def load_player_stats_for_league_season(
        self,
        league_code: str,
        season: str,
        stat_types: Optional[List[str]] = None,
        force_refresh: bool = False
    ) -> Dict[str, int]:
        """
        Load player statistics for a specific league and season.

        Args:
            league_code: League code (e.g., 'ENG-Premier League')
            season: Season name (e.g., '2020-2021')
            stat_types: List of stat types to load (default: all except goalkeeping)
            force_refresh: If True, refresh cached data

        Returns:
            Dictionary with counts of inserted/updated records
        """
        if stat_types is None:
            # Default to all except goalkeeping (which has different structure)
            stat_types = [s for s in self.STAT_TYPES if s != 'goalkeeping']

        self.logger.info(f"Loading player stats for {league_code} {season}")

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
                self.logger.info(f"Loading {stat_type} player stats...")

                # Start load logging
                load_id = self.load_logger.start_load(
                    source_name='FBref',
                    load_type=f'player_stats_{stat_type}',
                    target_table='player_season_stats',
                    league_season_id=league_season_id
                )

                try:
                    # Fetch player stats
                    stats_df = fbref.read_player_season_stats(stat_type=stat_type)

                    if stats_df.empty:
                        self.logger.warning(f"No {stat_type} player stats found")
                        self.load_logger.complete_load(status='completed')
                        continue

                    self.logger.info(f"Found {len(stats_df)} player records")

                    # Process and insert stats
                    stats = self._process_player_stats(
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
                        f"Loading {stat_type} player stats for {league_code} {season}"
                    )
                    continue

            return total_stats

        except Exception as e:
            self.logger.error(f"Player stats load failed: {e}", exc_info=True)
            raise

    def _process_player_stats(
        self,
        stats_df: pd.DataFrame,
        league_season_id: int,
        stat_type: str
    ) -> Dict[str, int]:
        """
        Process and insert player statistics.

        Args:
            stats_df: DataFrame with player stats
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

        for idx, row in stats_df.iterrows():
            try:
                # Extract player and team info
                player_name = row.get('player', row.get('Player'))
                team_name = row.get('team', row.get('Squad'))

                if pd.isna(player_name) or pd.isna(team_name):
                    failed += 1
                    continue

                # Get or create player
                player_kwargs = {}
                if 'nationality' in row or 'Nation' in row:
                    nationality = row.get('nationality', row.get('Nation'))
                    if pd.notna(nationality):
                        player_kwargs['nationality'] = str(nationality)[:50]

                if 'position' in row or 'Pos' in row:
                    position = row.get('position', row.get('Pos'))
                    if pd.notna(position):
                        # Clean position (might have format like "FW,MF")
                        pos = str(position).split(',')[0].strip()[:20]
                        player_kwargs['position'] = pos

                player_id = self.db.get_or_create_player(
                    str(player_name),
                    **player_kwargs
                )

                # Get or create team
                team_id = self.db.get_or_create_team(str(team_name))

                # Extract stats based on type
                stats_data = self._extract_player_stats(row, stat_type)

                # Prepare upsert data
                upsert_data = [(
                    player_id,
                    team_id,
                    league_season_id,
                    stat_type,
                    stats_data.get('matches_played'),
                    stats_data.get('starts'),
                    stats_data.get('minutes_played'),
                    stats_data.get('goals'),
                    stats_data.get('assists'),
                    stats_data.get('penalty_goals'),
                    stats_data.get('penalty_attempts'),
                    stats_data.get('shots'),
                    stats_data.get('shots_on_target'),
                    stats_data.get('shots_on_target_pct'),
                    stats_data.get('goals_per_shot'),
                    stats_data.get('passes_completed'),
                    stats_data.get('passes_attempted'),
                    stats_data.get('pass_completion_pct'),
                    stats_data.get('key_passes'),
                    stats_data.get('tackles'),
                    stats_data.get('tackles_won'),
                    stats_data.get('interceptions'),
                    stats_data.get('blocks'),
                    stats_data.get('clearances'),
                    stats_data.get('yellow_cards'),
                    stats_data.get('red_cards'),
                    stats_data.get('fouls_committed'),
                    stats_data.get('fouls_drawn'),
                    stats_data.get('saves'),
                    stats_data.get('saves_pct'),
                    stats_data.get('clean_sheets'),
                    stats_data.get('goals_against'),
                    json.dumps(stats_data.get('additional_stats', {}))
                )]

                rows_affected = self.db.upsert(
                    table='player_season_stats',
                    columns=[
                        'player_id', 'team_id', 'league_season_id', 'stat_type',
                        'matches_played', 'starts', 'minutes_played',
                        'goals', 'assists', 'penalty_goals', 'penalty_attempts',
                        'shots', 'shots_on_target', 'shots_on_target_pct',
                        'goals_per_shot', 'passes_completed', 'passes_attempted',
                        'pass_completion_pct', 'key_passes', 'tackles',
                        'tackles_won', 'interceptions', 'blocks', 'clearances',
                        'yellow_cards', 'red_cards', 'fouls_committed',
                        'fouls_drawn', 'saves', 'saves_pct', 'clean_sheets',
                        'goals_against', 'additional_stats'
                    ],
                    data=upsert_data,
                    conflict_columns=['player_id', 'team_id', 'league_season_id', 'stat_type']
                )

                if rows_affected > 0:
                    updated += 1
                else:
                    inserted += 1

            except Exception as e:
                self.logger.error(f"Error processing player stats at row {idx}: {e}")
                failed += 1
                continue

        self.logger.info(
            f"Player stats processing complete: {inserted} inserted, "
            f"{updated} updated, {failed} failed"
        )

        return {
            'inserted': inserted,
            'updated': updated,
            'failed': failed
        }

    def _extract_player_stats(self, row: pd.Series, stat_type: str) -> Dict:
        """
        Extract player statistics from row based on stat type.

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

        # Common stats across types
        stats['matches_played'] = get_val(['games', 'MP', 'Matches'], int)
        stats['starts'] = get_val(['games_starts', 'Starts'], int)
        stats['minutes_played'] = get_val(['minutes', 'Min', 'Minutes'], int)

        # Standard stats
        if stat_type == 'standard':
            stats['goals'] = get_val(['goals', 'Gls', 'Goals'], int)
            stats['assists'] = get_val(['assists', 'Ast', 'Assists'], int)
            stats['penalty_goals'] = get_val(['pens_made', 'PK'], int)
            stats['penalty_attempts'] = get_val(['pens_att', 'PKatt'], int)
            stats['yellow_cards'] = get_val(['cards_yellow', 'CrdY'], int)
            stats['red_cards'] = get_val(['cards_red', 'CrdR'], int)
            stats['additional_stats']['expected_goals'] = get_val(['xg', 'xG'], float)
            stats['additional_stats']['expected_assists'] = get_val(['xg_assist', 'xAG'], float)

        # Shooting stats
        elif stat_type == 'shooting':
            stats['shots'] = get_val(['shots', 'Sh'], int)
            stats['shots_on_target'] = get_val(['shots_on_target', 'SoT'], int)
            stats['shots_on_target_pct'] = get_val(['shots_on_target_pct', 'SoT%'], float)
            stats['goals_per_shot'] = get_val(['goals_per_shot', 'G/Sh'], float)
            stats['additional_stats']['shots_per_90'] = get_val(['shots_per90'], float)

        # Passing stats
        elif stat_type == 'passing':
            stats['passes_completed'] = get_val(['passes_completed', 'Cmp'], int)
            stats['passes_attempted'] = get_val(['passes', 'Att'], int)
            stats['pass_completion_pct'] = get_val(['passes_pct', 'Cmp%'], float)
            stats['key_passes'] = get_val(['assisted_shots', 'KP'], int)
            stats['additional_stats']['progressive_passes'] = get_val(['progressive_passes'], int)

        # Defensive stats
        elif stat_type == 'defense':
            stats['tackles'] = get_val(['tackles', 'Tkl'], int)
            stats['tackles_won'] = get_val(['tackles_won', 'TklW'], int)
            stats['interceptions'] = get_val(['interceptions', 'Int'], int)
            stats['blocks'] = get_val(['blocks', 'Blocks'], int)
            stats['clearances'] = get_val(['clearances', 'Clr'], int)

        # Possession stats
        elif stat_type == 'possession':
            stats['additional_stats']['touches'] = get_val(['touches', 'Touches'], int)
            stats['additional_stats']['progressive_carries'] = get_val(['progressive_carries'], int)
            stats['additional_stats']['dribbles_completed'] = get_val(['take_ons_won'], int)

        # Goalkeeping stats
        elif stat_type == 'goalkeeping':
            stats['saves'] = get_val(['gk_saves', 'Saves'], int)
            stats['saves_pct'] = get_val(['gk_save_pct', 'Save%'], float)
            stats['clean_sheets'] = get_val(['gk_clean_sheets', 'CS'], int)
            stats['goals_against'] = get_val(['gk_goals_against', 'GA'], int)
            stats['additional_stats']['penalty_saves'] = get_val(['gk_pens_save'], int)

        # Fouls
        stats['fouls_committed'] = get_val(['fouls', 'Fls'], int)
        stats['fouls_drawn'] = get_val(['fouled', 'Fld'], int)

        # Store all other columns in additional_stats
        exclude_cols = ['player', 'Player', 'team', 'Squad', 'nationality', 'Nation', 'position', 'Pos']
        for col in row.index:
            if col not in exclude_cols and col not in stats:
                val = row[col]
                if pd.notna(val):
                    # Convert to Python native types for JSON serialization
                    if isinstance(val, (pd.Timestamp, pd.Period)):
                        stats['additional_stats'][col] = str(val)
                    elif isinstance(val, (int, float, str, bool)):
                        stats['additional_stats'][col] = val
                    else:
                        stats['additional_stats'][col] = str(val)

        return stats


def main():
    """Main function for command-line usage."""
    import argparse

    parser = argparse.ArgumentParser(description='Load player statistics into database')
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
        choices=PlayerStatsLoader.STAT_TYPES,
        help='Stat types to load (default: all except goalkeeping)'
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
        loader = PlayerStatsLoader(db, logger)

        # Load player stats
        stats = loader.load_player_stats_for_league_season(
            league_code=args.league,
            season=args.season,
            stat_types=args.stat_types,
            force_refresh=args.force_refresh
        )

        logger.info(f"Player stats load complete: {stats}")

    except Exception as e:
        logger.error(f"Player stats load failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
