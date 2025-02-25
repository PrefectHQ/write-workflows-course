import requests
from datetime import datetime, timedelta
import json


def get_nationals_recent_game():
    """Get stats for the most recent Washington Nationals game"""
    # Get current date
    today = datetime.now()

    # First get the current season schedule
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

    print("Boxscore:", boxscore)
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
        result["batting"] = {
            "runs": nats_stats["batting"]["runs"],
            "hits": nats_stats["batting"]["hits"],
            "home_runs": nats_stats["batting"]["homeRuns"],
            "avg": nats_stats["batting"]["avg"],
        }

        # Add pitching stats
        result["pitching"] = {
            "earned_runs": nats_stats["pitching"]["earnedRuns"],
            "strikeouts": nats_stats["pitching"]["strikeOuts"],
            "walks": nats_stats["pitching"]["baseOnBalls"],
            "era": nats_stats["pitching"]["era"],
        }

        # Get top performers
        nats_batters = boxscore["teams"][nats_side]["players"]

        # Find best hitter (most hits)
        best_hitter = {"name": "None", "hits": 0, "at_bats": 0}
        for player_id, player in nats_batters.items():
            if "stats" in player and "batting" in player["stats"]:
                if player["stats"]["batting"].get("hits", 0) > best_hitter["hits"]:
                    best_hitter = {
                        "name": player["person"]["fullName"],
                        "hits": player["stats"]["batting"].get("hits", 0),
                        "at_bats": player["stats"]["batting"].get("atBats", 0),
                    }

        result["top_hitter"] = best_hitter

    except KeyError as e:
        result["stats_error"] = f"Could not retrieve complete stats: {str(e)}"

    return result


if __name__ == "__main__":
    game_stats = get_nationals_recent_game()
    print(json.dumps(game_stats, indent=2))


# from prefect import flow, task
# import requests
# from datetime import datetime, timedelta
# import json
# import pandas as pd


# @task(name="fetch_most_recent_game", retries=3, retry_delay_seconds=30)
# def fetch_most_recent_game():
#     """Fetch the most recent Washington Nationals game."""
#     # Get today's date
#     today = datetime.now()

#     # Start looking from today and go back up to 7 days to find the most recent game
#     for i in range(7):
#         date = today - timedelta(days=i)
#         date_str = date.strftime("%Y-%m-%d")

#         url = f"https://statsapi.mlb.com/api/v1/schedule"
#         params = {
#             "teamId": 120,  # Washington Nationals team ID
#             "sportId": 1,  # MLB
#             "date": date_str,
#         }

#         response = requests.get(url, params=params)
#         response.raise_for_status()

#         data = response.json()

#         # Check if there were any games on this date
#         if data.get("dates") and data["dates"][0].get("games"):
#             games = data["dates"][0]["games"]
#             for game in games:
#                 # Only consider completed games
#                 if game["status"]["detailedState"] == "Final":
#                     return {
#                         "game_id": game["gamePk"],
#                         "date": date_str,
#                         "home_team": game["teams"]["home"]["team"]["name"],
#                         "away_team": game["teams"]["away"]["team"]["name"],
#                         "home_score": game["teams"]["home"]["score"],
#                         "away_score": game["teams"]["away"]["score"],
#                     }

#     # If no recent completed game was found
#     return None


# @task(name="fetch_win_probability", retries=3, retry_delay_seconds=30)
# def fetch_win_probability(game_id):
#     """Fetch the pre-game win probability for the Nationals for a specific game."""
#     url = f"https://statsapi.mlb.com/api/v1/game/{game_id}/winProbability"

#     response = requests.get(url)
#     response.raise_for_status()

#     data = response.json()

#     # Get the first win probability entry (pre-game)
#     if data and len(data) > 0:
#         # Check if we can determine if Nationals are home or away
#         game_url = f"https://statsapi.mlb.com/api/v1/game/{game_id}/boxscore"
#         game_response = requests.get(game_url)
#         game_response.raise_for_status()
#         game_data = game_response.json()

#         home_team_id = game_data["teams"]["home"]["team"]["id"]
#         nationals_id = 120  # Washington Nationals team ID

#         is_nationals_home = home_team_id == nationals_id
#         print("Is Nationals home:", is_nationals_home)

#         # Get home win probability
#         home_win_prob = data[0].get("homeWinProbability", 0)
#         print("Home win probability:", home_win_prob)

#         # Determine Nationals win probability
#         nats_win_prob = home_win_prob if is_nationals_home else (1 - home_win_prob)
#         print("Nationals win probability:", nats_win_prob)

#         return {
#             "nationals_win_probability": nats_win_prob,
#             "is_nationals_home": is_nationals_home,
#             "home_win_probability": home_win_prob,
#         }

#     # Default if no data is found
#     return {
#         "nationals_win_probability": None,
#         "is_nationals_home": None,
#         "home_win_probability": 0.5,
#     }


# @task(name="determine_actual_result")
# def determine_actual_result(game_data):
#     """Determine if the Nationals won the game based on the score."""
#     if game_data["home_team"] == "Washington Nationals":
#         return game_data["home_score"] > game_data["away_score"]
#     else:
#         return game_data["away_score"] > game_data["home_score"]


# @flow(name="Recent Nationals Game Win Probability")
# def recent_game_probability_flow():
#     """
#     Prefect flow to get the win probability for the most recent Washington Nationals game.

#     This flow:
#     1. Fetches the most recent completed Nationals game
#     2. Gets the pre-game win probability
#     3. Determines if the Nationals actually won
#     4. Returns all the information in a structured format
#     """
#     # Step 1: Fetch the most recent game
#     game_data = fetch_most_recent_game()

#     if not game_data:
#         print("No recent completed Nationals games found.")
#         return {
#             "status": "No recent games found",
#             "timestamp": datetime.now().isoformat(),
#         }

#     # Step 2: Get win probability
#     win_prob_data = fetch_win_probability(game_data["game_id"])

#     # Step 3: Determine actual result
#     nationals_won = determine_actual_result(game_data)

#     # Step 4: Prepare and return the results
#     result = {
#         "game_id": game_data["game_id"],
#         "date": game_data["date"],
#         "matchup": f"{game_data['away_team']} @ {game_data['home_team']}",
#         "final_score": f"{game_data['away_team']} {game_data['away_score']} - {game_data['home_team']} {game_data['home_score']}",
#         "nationals_win_probability": win_prob_data["nationals_win_probability"],
#         "nationals_actually_won": nationals_won,
#         "prediction_correct": (win_prob_data["nationals_win_probability"] > 0.5)
#         == nationals_won,
#         "timestamp": datetime.now().isoformat(),
#     }

#     # Print out results in a nice format
#     print("\n" + "=" * 50)
#     print(f"WASHINGTON NATIONALS RECENT GAME ANALYSIS")
#     print("=" * 50)
#     print(f"Game: {result['matchup']} on {result['date']}")
#     print(f"Final Score: {result['final_score']}")
#     print(
#         f"Pre-game Win Probability for Nationals: {result['nationals_win_probability']:.1%}"
#     )
#     print(f"Nationals {'Won' if result['nationals_actually_won'] else 'Lost'} the game")
#     print(
#         f"Prediction was {'CORRECT' if result['prediction_correct'] else 'INCORRECT'}"
#     )
#     print("=" * 50 + "\n")

#     # Create a DataFrame for easy CSV export if needed
#     df = pd.DataFrame([result])

#     return result


# if __name__ == "__main__":
#     recent_game_probability_flow()
