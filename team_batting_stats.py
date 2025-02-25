import httpx
from datetime import datetime


def get_nationals_most_recent_game():
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
        ],  # Regular season & postseason games
    }

    schedule_response = httpx.get(schedule_url, schedule_params)
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

                    # If we haven't found a game yet, or the current one is more recent
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

    return most_recent_game, most_recent_date


def get_game_data(most_recent_game: dict, most_recent_date: datetime) -> dict:
    """Get detailed box score stats for a given game ID"""

    game_id = most_recent_game["gamePk"]

    # Get detailed box score stats
    boxscore_url = f"https://statsapi.mlb.com/api/v1/game/{game_id}/boxscore"
    boxscore_response = httpx.get(boxscore_url)
    boxscore = boxscore_response.json()

    # Determine if Nationals are home or away
    is_home = most_recent_game["teams"]["home"]["team"]["id"] == 120
    nats_side = "home" if is_home else "away"
    opponent_side = "away" if is_home else "home"

    # Store basic game info in a dictionary
    result = {
        "game_id": game_id,
        "date": most_recent_date,
        "opponent": most_recent_game["teams"][opponent_side]["team"]["name"],
        "status": most_recent_game["status"]["detailedState"],
        "score": f"Nationals {most_recent_game['teams'][nats_side]['score']} - {most_recent_game['teams'][opponent_side]['team']['name']} {most_recent_game['teams'][opponent_side]['score']}",
    }

    # Add win/loss information
    if most_recent_game["teams"][nats_side]["isWinner"]:
        result["result"] = "WIN"
    else:
        result["result"] = "LOSS"

    # Store Nationals team batt stats
    try:
        nats_stats = boxscore["teams"][nats_side]["teamStats"]
        result["team_batting"] = {
            "runs": nats_stats["batting"]["runs"],
            "hits": nats_stats["batting"]["hits"],
            "home_runs": nats_stats["batting"]["homeRuns"],
        }

    except KeyError as e:
        result["stats_error"] = f"Could not retrieve complete stats: {str(e)}"

    return result


def print_batting_stats(stats):
    """Print batting stats in a nicely formatted table"""

    if "error" in stats:
        print(f"ERROR: {stats['error']}")
        return
    print("\n" + "=" * 80)
    print(f"WASHINGTON NATIONALS - {stats['date']} vs {stats['opponent']}")
    print(f"Result: {stats['result']} - {stats['score']}")
    print("=" * 80)
    print(
        f"\nTEAM BATTING: {stats['team_batting']['hits']} Hits, {stats['team_batting']['runs']} Runs, {stats['team_batting']['home_runs']} HR"
    )


def assemble_game_stats():
    """Get and print game stats for most recent Nationals game"""
    most_recent_game, most_recent_date = get_nationals_most_recent_game()
    game_data = get_game_data(most_recent_game, most_recent_date)
    print_batting_stats(game_data)
    return


if __name__ == "__main__":
    assemble_game_stats()
