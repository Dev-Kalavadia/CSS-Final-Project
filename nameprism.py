import requests
import pandas as pd
import time
import ast
from tqdm import tqdm

# API key is already provided
nameprism_api_key = '56fbbabfa1e49f05'

# Function to get ethnicity data from NamePrism API
def get_ethnicity(name):
    url = f'https://www.name-prism.com/api_token/eth/json/{nameprism_api_key}/{name}'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to get ethnicity for {name}: {response.text}")
        return None

# Load the movie data CSV file
df = pd.read_csv("Data/Final/Output/output_movies_data_25k-35k.csv")

# Assuming the cast members are in the 10th column (index 9) and are a string representation of a list
df['cast'] = df.iloc[:, 8].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else [])

# Prepare a new DataFrame to store cast member names and ethnicities
cast_ethnicity_df = pd.DataFrame(columns=['Name', '2PRACE', 'Hispanic', 'API', 'Black', 'AIAN', 'White'])

# Define the start and end range for processing
start_movie_index = 3052  # Modify this as needed
end_movie_index = 5128   # Modify this as needed
# 5128

# Check if there's a saved checkpoint
try:
    cast_ethnicity_df = pd.read_csv("Data/Final/Ethnicity/cast_ethnicity_checkpoint.csv")
    print("Loaded checkpoint data.")
except FileNotFoundError:
    print("No checkpoint found. Starting from scratch.")

# Loop through the dataframe within the defined range
for index, row in tqdm(df.iloc[start_movie_index:end_movie_index].iterrows(), total=end_movie_index-start_movie_index, desc="Processing movies", unit="movie"):
    # Extract up to 5 cast members
    cast_members = row['cast'][:5]

    # Get ethnicity data for each cast member
    for cast_member in cast_members:
        # Skip if the cast member is already in the DataFrame
        if cast_member in cast_ethnicity_df['Name'].values:
            print(f"Skipping {cast_member} because it already exists in the DataFrame.")
            continue

        try:
            # Replace spaces with %20 for URL encoding
            encoded_name = cast_member.replace(' ', '%20')
            ethnicity = get_ethnicity(encoded_name)
            if ethnicity:
                # Create a new row with the cast member's name and ethnicities
                new_row = pd.DataFrame([{'Name': cast_member, **ethnicity}])
                # Append the new row to the cast_ethnicity_df DataFrame using concat
                cast_ethnicity_df = pd.concat([cast_ethnicity_df, new_row], ignore_index=True)
        except Exception as e:
            print(f"An error occurred while processing {cast_member}: {e}")
            continue  # Skip to the next cast member

        # Sleep to respect the API rate limit
        time.sleep(1)  # 60 calls per minute means 1 call per second

    # Save a checkpoint every 50 movies
    if (index + 1 - start_movie_index) % 50 == 0:
        cast_ethnicity_df.to_csv("Data/Final/Ethnicity/cast_ethnicity_checkpoint.csv", index=False)
        print(f"Checkpoint saved at movie {index + 1}.")

# Save the final DataFrame to a CSV file
cast_ethnicity_df.to_csv("Data/Final/Ethnicity/cast_ethnicity.csv", index=False)
print("Processing complete. Final data saved.")
