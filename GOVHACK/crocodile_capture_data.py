import pandas as pd  # Import pandas for data manipulation
import folium  # Import folium for creating interactive maps

# Function to create the map with markers
def create_map(data_df, coordinates_df, output_file='crocodile_capture_map.html'):
    """
    Create an interactive map with markers representing crocodile captures in different zones.

    Parameters:
    - data_df: DataFrame containing capture data including zone names and dates.
    - coordinates_df: DataFrame containing zone names and their corresponding latitude and longitude.
    - output_file: Name of the HTML file to save the map.
    """
    # Define a base map centered on Northern Territory with an initial zoom level of 6
    map_center = [-12.4634, 130.8456]  # Approximate central coordinates of Northern Territory
    map_obj = folium.Map(location=map_center, zoom_start=6)  # Create a folium map object centered at 'map_center'

    # Merge capture data with coordinates based on 'ZONE_NAME'
    merged_df = pd.merge(data_df, coordinates_df, on='ZONE_NAME')  # Join data_df and coordinates_df on 'ZONE_NAME'

    # Print columns in merged DataFrame for debugging purposes
    print("Columns in merged DataFrame:", merged_df.columns.tolist())  # Display column names for verification

    # Add markers to the map for each zone
    for _, row in merged_df.iterrows():  # Iterate over each row in the merged DataFrame
        try:
            zone_name = row['ZONE_NAME']  # Extract the zone name
            total_captures = row['YEAR_TOTAL']  # Extract the total captures for the zone
            latitude = row['LATITUDE']  # Extract the latitude for the zone
            longitude = row['LONGITUDE']  # Extract the longitude for the zone

            # Add a marker to the map with a popup showing zone and total captures
            folium.Marker(
                location=[latitude, longitude],  # Set marker location
                popup=f"Zone: {zone_name}<br>Total Captures: {total_captures}",  # Popup text
                icon=folium.Icon(color='red' if total_captures > 50 else 'blue')  # Set marker color based on capture count
            ).add_to(map_obj)  # Add the marker to the map
        except KeyError as e:
            print(f"Missing key: {e}")  # Handle the case where expected columns are missing

    # Save the map as an HTML file
    map_obj.save(output_file)  # Save the map to the specified HTML file
    print(f"Map with crocodile capture zones saved as {output_file}.")  # Confirm saving

# Load capture data from CSV file
capture_data_file = 'crocodile_capture_data.csv'  # Define the path to the CSV file containing capture data
data_df = pd.read_csv(capture_data_file)  # Read the CSV file into a DataFrame

# Print column names for debugging purposes
print("Columns in the CSV file:", data_df.columns.tolist())  # Display column names to verify data structure

# Convert 'DATE_CAPTURED' column to datetime format
data_df['DATE_CAPTURED'] = pd.to_datetime(data_df['DATE_CAPTURED'])  # Ensure 'DATE_CAPTURED' is in datetime format

# Extract year from 'DATE_CAPTURED'
data_df['YEAR'] = data_df['DATE_CAPTURED'].dt.year  # Create a new column 'YEAR' from 'DATE_CAPTURED'

# Group by year and zone to count captures, then sum up to get total captures per zone
yearly_totals = data_df.groupby(['YEAR', 'ZONE_NAME']).size().unstack(fill_value=0)  # Count captures per year and zone
data_df['YEAR_TOTAL'] = yearly_totals.sum(axis=1).reindex(data_df['ZONE_NAME']).values  # Sum totals for each zone

# Define coordinates for each zone
coordinates_data = {
    'ZONE_NAME': ['Borroloola', 'Katherine Zone', 'Litchfield', 'Lower Harbour', 'Management Zone',
                  'Nhulunbuy', 'Outside Management Zone', 'Shoal Bay', 'Upper Harbour'],  # List of zone names
    'LATITUDE': [-17.7414, -14.4846, -13.0876, -12.4634, -12.4640, -12.6461, -12.8000, -12.8456, -12.9000],  # Latitudes
    'LONGITUDE': [139.3268, 132.4603, 130.9075, 130.8456, 130.8460, 136.8121, 136.8700, 131.3870, 131.4000]  # Longitudes
}

# Convert coordinates data to DataFrame
coordinates_df = pd.DataFrame(coordinates_data)  # Create a DataFrame from the coordinates dictionary

# Create the map with the provided data
create_map(data_df, coordinates_df)  # Call the function to create the map and save it
