import requests
import pandas as pd
import s3fs 

def Extract_Data():

    b = requests.get("https://api.nasa.gov/neo/rest/v1/feed?start_date=2023-09-20&end_date=2023-09-21&api_key=DEMO_KEY")

    data = b.json()

    all_names = []
    all_magnitudes = []
    all_diameter_min = []
    all_diameter_max = []
    all_is_dangerous = []
    all_close_approach_dates = []
    all_kim = []
    all_miss = []

    # Loop through all dates and extract attributes
    for date, objects in data["near_earth_objects"].items():
        for entry in objects:
            all_names.append(entry["name"])
            all_magnitudes.append(entry["absolute_magnitude_h"])
            all_diameter_min.append(entry["estimated_diameter"]["kilometers"]["estimated_diameter_min"])
            all_diameter_max.append(entry["estimated_diameter"]["kilometers"]["estimated_diameter_max"])
            all_is_dangerous.append(entry["is_potentially_hazardous_asteroid"])
            all_close_approach_dates.append(entry["close_approach_data"][0]["close_approach_date_full"])
            all_kim.append(entry["close_approach_data"][0]["relative_velocity"]["kilometers_per_second"])
            all_miss.append(entry["close_approach_data"][0]["miss_distance"]["kilometers"])

    # Create a DataFrame
    df = pd.DataFrame({
        "Name": all_names,
        "Magnitude": all_magnitudes,
        "Diameter (min)": all_diameter_min,
        "Diameter (max)": all_diameter_max,
        "Is Dangerous": all_is_dangerous,
        "Close Approach Date": all_close_approach_dates,
        "Speed in KM": all_kim,
        "Miss Distance": all_miss
    })

    df.to_csv('s3://rajatasteroidairflow/asteroid.csv')
