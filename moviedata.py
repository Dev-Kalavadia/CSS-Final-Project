import requests
import json
import pandas as pd
import time
from tqdm import tqdm
import os
from requests.exceptions import ConnectionError, Timeout, SSLError

start_movie_index = 35000  # Modify this as needed
end_movie_index = 45000  # Modify this as needed

# Define the checkpoint interval
checkpoint_interval = 100

# Define the folder to store checkpoint files
checkpoint_folder = "Data/Final/Checkpoints"

def make_request_with_retries(url, max_retries=3, timeout=5):
    retry_count = 0
    while retry_count < max_retries:
        try:
            response = requests.get(url, timeout=timeout)
            return response
        except (ConnectionError, Timeout, SSLError) as e:
            print(f"Request failed: {e}. Retrying ({retry_count+1}/{max_retries})...")
            time.sleep(2**retry_count)  # Exponential backoff
            retry_count += 1
    return None  # or raise an exception


# Ensure the checkpoint folder exists
os.makedirs(checkpoint_folder, exist_ok=True)

# Function to load movies from file
def load_movies(filepath, start_index, end_index):
    with open(filepath, "r") as f:
        movies = [line.strip() for line in f.readlines()][start_index:end_index]
    return movies

# Function to save data to CSV in a folder
def save_to_csv(data, folder, filename):
    if not data:
        return  # If no data, don't save
    df = pd.DataFrame(data)
    filepath = os.path.join(folder, filename)
    df.to_csv(filepath, mode="a", header=not os.path.exists(filepath), index=False)

# Load movies and prepare the list
movies = load_movies("Data/Final/rotten_tomatoes_movies_english_title.txt", start_movie_index, end_movie_index)
movies = [movie.lower().replace(" ", "_") for movie in movies]

# Create an empty list to store movie data m
movie_data = []

# Initialize progress bar
pbar = tqdm(total=end_movie_index-start_movie_index, desc="Scraping movies", unit="movie")

# Loop through each movie title
# Loop through each movie title
for i, movie in enumerate(movies, start=start_movie_index):
    # Construct the request URL
    url = f'https://rotten-tomatoes-api.ue.r.appspot.com/movie/{movie}'
    
    try:
        # Respectful crawling by waiting 1 second between requests
        time.sleep(1)
        # Make the HTTP GET request
        response = requests.get(url)

        # Check if the response contains 'Not Found' or 'Internal Server Error'
        if response.status_code == 404 or response.status_code == 500:
            continue  # Skip this movie

        # Parse the JSON response
        data = response.json()

        # Append the movie data to the list
        movie_data.append(data)

        # Save a checkpoint every 'checkpoint_interval' movies
        if (i + 1) % checkpoint_interval == 0 or i + 1 == end_movie_index:
            checkpoint_filename = f"movies_{i + 1 - checkpoint_interval}-{i + 1}.csv"
            save_to_csv(movie_data, checkpoint_folder, checkpoint_filename)
            movie_data = []  # Reset the list after saving

    except requests.exceptions.RequestException as e:
        # Handle specific requests exceptions
        print(f"RequestException occurred while scraping {movie}: {e}")
    except json.JSONDecodeError as e:
        # Handle JSON decode errors
        print(f"JSONDecodeError occurred while scraping {movie}: {e}")
    except Exception as e:
        # Handle any other exceptions
        print(f"An unexpected error occurred while scraping {movie}: {e}")
    finally:
        # Update the progress bar
        pbar.update(1)


# Save any remaining movie data to CSV
if movie_data:
    checkpoint_filename = f"movies_{end_movie_index-len(movie_data)+1}-{end_movie_index}.csv"
    save_to_csv(movie_data, checkpoint_folder, checkpoint_filename)

pbar.close()
print("Scraping complete. Data saved.")
