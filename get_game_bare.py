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

    schedule_response = httpx.get(url=schedule_url, params=schedule_params)
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

    return most_recent_game


if __name__ == "__main__":
    most_recent_game = get_nationals_most_recent_game()
    print(most_recent_game)
