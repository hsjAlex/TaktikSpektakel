import requests
import json
from datetime import datetime

# Function to fetch user statistics from Lichess API

def fetch_user_stats(username):
    url = f'https://lichess.org/api/user/{username}'
    response = requests.get(url)
    return response.json()

# Function to fetch puzzle ratings from Lichess API

def fetch_puzzle_ratings(username):
    url = f'https://lichess.org/api/user/{username}/ratings/puzzle'
    response = requests.get(url)
    return response.json()

# List of team members from hessische-schachjugend
members = ['member1', 'member2', 'member3']  # Replace with actual member usernames

# Dictionary to hold all stats
all_stats = {}

# Fetch stats for each member
for member in members:
    user_stats = fetch_user_stats(member)
    puzzle_ratings = fetch_puzzle_ratings(member)
    all_stats[member] = {
        'user_stats': user_stats,
        'puzzle_ratings': puzzle_ratings
    }

# Define filename with timestamp
filename = f'tactic_stats_{datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")}.json'

# Save the stats to a JSON file
with open(filename, 'w') as json_file:
    json.dump(all_stats, json_file, indent=4)
