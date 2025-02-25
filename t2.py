import requests
from datetime import datetime, timedelta
import json


def get_nationals_recent_game():
    """Get stats for the most recent Washington Nationals game"""

    today = datetime.now()

    # Get the current season schedule
    schedule_url = "https://statsapi.mlb.com/api/v1/schedule"
    schedule_params = {
        "teamId": 120,  # Washington Nationals team ID
        "sportId": 1,  # MLB
        "startDate": "2024-01-01",
        "endDate": today.strftime("%Y-%m-%d"),
        "gameType": [
            "R",
            "P",
            "S",
        ],  # Regular season, postseason, and spring training games
    }

    schedule_response = requests.get(schedule_url, schedule_params)
    schedule_data = schedule_response.json()

    # Find the most recent completed game
    most_recent_game = None
    most_recent_date = None

    if "dates" in schedule_data:
        # Sort dates in reverse chronological order
        sorted_dates = sorted(
            schedule_data["dates"], key=lambda x: x["date"], reverse=True
        )

        for date_data in sorted_dates:
            for game in date_data["games"]:
                # Check if the game is completed
                if game["status"]["abstractGameState"] == "Final":
                    game_date = datetime.strptime(date_data["date"], "%Y-%m-%d")

                    # If we haven't found a game yet, or this one is more recent
                    if most_recent_date is None or game_date > most_recent_date:
                        most_recent_game = game
                        most_recent_date = game_date

                        # Since we're already sorted, the first Final game we find is most recent
                        break

            # If we found a game on this date, we're done
            if most_recent_game:
                break

    # If no completed games found
    if not most_recent_game:
        return {"error": "No recent completed games found"}

    # Get the game ID and fetch detailed stats
    game_id = most_recent_game["gamePk"]

    # Get detailed box score stats
    boxscore_url = f"https://statsapi.mlb.com/api/v1/game/{game_id}/boxscore"
    boxscore_response = requests.get(boxscore_url)
    boxscore = boxscore_response.json()

    # Determine if Nationals are home or away
    is_home = most_recent_game["teams"]["home"]["team"]["id"] == 120
    nats_side = "home" if is_home else "away"
    opponent_side = "away" if is_home else "home"

    # Get basic game info
    result = {
        "game_id": game_id,
        "date": most_recent_date.strftime("%Y-%m-%d"),
        "opponent": most_recent_game["teams"][opponent_side]["team"]["name"],
        "venue": (
            most_recent_game["venue"]["name"]
            if "venue" in most_recent_game
            else "Unknown"
        ),
        "status": most_recent_game["status"]["detailedState"],
        "score": f"Nationals {most_recent_game['teams'][nats_side]['score']} - {most_recent_game['teams'][opponent_side]['team']['name']} {most_recent_game['teams'][opponent_side]['score']}",
    }

    # Add win/loss information
    if most_recent_game["teams"][nats_side]["isWinner"]:
        result["result"] = "WIN"
    else:
        result["result"] = "LOSS"

    # Get Nationals team stats
    try:
        nats_stats = boxscore["teams"][nats_side]["teamStats"]

        # Add batting stats
        result["team_batting"] = {
            "runs": nats_stats["batting"]["runs"],
            "hits": nats_stats["batting"]["hits"],
            "home_runs": nats_stats["batting"]["homeRuns"],
            "avg": nats_stats["batting"]["avg"],
        }

        # Get individual player stats
        nats_players = boxscore["teams"][nats_side]["players"]

        # Store all batters with stats
        result["player_batting_stats"] = []

        for player_id, player in nats_players.items():
            # Check if player has batting stats
            if (
                "stats" in player
                and "batting" in player["stats"]
                and "atBats" in player["stats"]["batting"]
            ):
                # Only include players who had at least one at-bat
                if player["stats"]["batting"]["atBats"] > 0:
                    batting_stats = {
                        "name": player["person"]["fullName"],
                        "position": player.get("position", {}).get("abbreviation", ""),
                        "at_bats": player["stats"]["batting"].get("atBats", 0),
                        "runs": player["stats"]["batting"].get("runs", 0),
                        "hits": player["stats"]["batting"].get("hits", 0),
                        "doubles": player["stats"]["batting"].get("doubles", 0),
                        "triples": player["stats"]["batting"].get("triples", 0),
                        "home_runs": player["stats"]["batting"].get("homeRuns", 0),
                        "rbis": player["stats"]["batting"].get("rbi", 0),
                        "walks": player["stats"]["batting"].get("baseOnBalls", 0),
                        "strikeouts": player["stats"]["batting"].get("strikeOuts", 0),
                        "stolen_bases": player["stats"]["batting"].get(
                            "stolenBases", 0
                        ),
                        "avg": player["seasonStats"]["batting"].get("avg", ".000"),
                        "obp": player["seasonStats"]["batting"].get("obp", ".000"),
                        "slg": player["seasonStats"]["batting"].get("slg", ".000"),
                        "ops": player["seasonStats"]["batting"].get("ops", ".000"),
                    }
                    result["player_batting_stats"].append(batting_stats)

        # Sort players by batting order
        result["player_batting_stats"].sort(
            key=lambda x: (x.get("position") != "P", x.get("at_bats")), reverse=True
        )

        # Find best hitter (most hits)
        if result["player_batting_stats"]:
            best_hitter = max(result["player_batting_stats"], key=lambda x: x["hits"])
            result["top_hitter"] = best_hitter

    except KeyError as e:
        result["stats_error"] = f"Could not retrieve complete stats: {str(e)}"

    return result


def print_batting_stats(stats):
    """Print batting stats in a nicely formatted table"""
    if "error" in stats:
        print(f"ERROR: {stats['error']}")
        return

    # Print game info
    print("\n" + "=" * 80)
    print(f"WASHINGTON NATIONALS - {stats['date']} vs {stats['opponent']}")
    print(f"Result: {stats['result']} - {stats['score']}")
    print(f"Venue: {stats['venue']}")
    print("=" * 80)

    # Print team batting stats
    print(
        f"\nTEAM BATTING: {stats['team_batting']['hits']} Hits, {stats['team_batting']['runs']} Runs, {stats['team_batting']['home_runs']} HR"
    )

    # Print individual batting stats
    print("\nINDIVIDUAL BATTING STATS:")
    print(
        "{:<20} {:<5} {:<3} {:<3} {:<3} {:<3} {:<3} {:<3} {:<3} {:<3} {:<4} {:<5}".format(
            "PLAYER", "POS", "AB", "R", "H", "2B", "3B", "HR", "RBI", "BB", "SO", "AVG"
        )
    )
    print("-" * 80)

    for player in stats["player_batting_stats"]:
        print(
            "{:<20} {:<5} {:<3} {:<3} {:<3} {:<3} {:<3} {:<3} {:<3} {:<3} {:<4} {:<5}".format(
                player["name"],
                player["position"],
                player["at_bats"],
                player["runs"],
                player["hits"],
                player["doubles"],
                player["triples"],
                player["home_runs"],
                player["rbis"],
                player["walks"],
                player["strikeouts"],
                player["avg"],
            )
        )

    print("\n")


if __name__ == "__main__":
    game_stats = get_nationals_recent_game()

    # Print formatted batting stats
    print_batting_stats(game_stats)
