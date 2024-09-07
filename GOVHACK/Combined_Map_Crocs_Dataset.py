import pandas as pd
import folium
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
import time

def create_map():
    """
    Create a blank map object centered at latitude 0, longitude 0 with zoom level 2.
    This provides a base map for adding markers.
    """
    return folium.Map(location=[0, 0], zoom_start=2)  # Set initial location and zoom level

def add_marker_to_map(map_obj, lat, lon, address):
    """
    Add a marker to the provided map object at the specified latitude and longitude.
    The marker will display a popup with the address when clicked.
    
    Parameters:
    - map_obj: The folium map object to which the marker will be added.
    - lat: Latitude of the marker.
    - lon: Longitude of the marker.
    - address: Address text to be displayed in the popup.
    """
    folium.Marker(
        location=[lat, lon],
        popup=folium.Popup(f"Address: {address}", max_width=300)
    ).add_to(map_obj)

def reverse_geocode_with_retry(geolocator, coordinates, retries=3, timeout=10):
    """
    Reverse geocode a set of coordinates with retry logic in case of timeouts or service unavailability.
    
    Parameters:
    - geolocator: The geopy geolocator instance.
    - coordinates: A tuple (latitude, longitude) to reverse geocode.
    - retries: Number of times to retry the request in case of a timeout.
    - timeout: Time in seconds to wait for the geocoding service response.
    
    Returns:
    - Location object if successful, or None if all retries fail.
    """
    for attempt in range(retries):
        try:
            location = geolocator.reverse(coordinates, timeout=timeout)
            return location
        except GeocoderTimedOut:
            # If a timeout occurs, retry after an exponential backoff delay
            print(f"Geocoding service timed out on attempt {attempt + 1}. Retrying...")
            time.sleep(2 ** attempt)  # Exponential backoff
        except GeocoderUnavailable as e:
            # If the service is unavailable, print the error and stop retrying
            print(f"Geocoding service is unavailable: {e}")
            break  # Stop retrying if the service is unavailable
    return None

def process_csv(file_path, batch_size=100):
    """
    Process a CSV file containing location data, reverse geocode coordinates, 
    and create a map with markers for each location.
    
    Parameters:
    - file_path: Path to the CSV file containing location data.
    - batch_size: Number of rows to process in each batch to avoid overwhelming the server.
    """
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file_path)
    
    # Print column names for debugging purposes
    print("Columns in the CSV file:", df.columns.tolist())
    
    # Print first few rows of the DataFrame for debugging
    print("First few rows of the DataFrame:")
    print(df.head())
    
    # Initialize geolocator with a descriptive user-agent
    geolocator = Nominatim(user_agent="your_application_name_here")
    
    # Create a map object
    map_obj = create_map()
    
    # Process data in batches to avoid overwhelming the geocoding service
    for start in range(0, len(df), batch_size):
        end = min(start + batch_size, len(df))
        batch_df = df.iloc[start:end]
        
        # Process each row in the batch
        for index, row in batch_df.iterrows():
            try:
                # Convert 'name' column to string to avoid potential errors
                name = str(row['name'])
                lat = row['latitude']
                lon = row['longitude']
                
                # Get address from latitude and longitude with retry mechanism
                location = reverse_geocode_with_retry(geolocator, (lat, lon))
                address = location.address if location else "Address not found"
                
                # Add a marker to the map for this location
                add_marker_to_map(map_obj, lat, lon, address)
                
                print(f"Marker for {name} added to the map")
            except KeyError as e:
                # Handle cases where expected columns are missing
                print(f"Column not found: {e}")
        
        # Optional: sleep between batches to avoid hitting rate limits of the geocoding service
        time.sleep(2)
    
    # Save the final map to an HTML file
    output_file = 'combined_map.html'
    map_obj.save(output_file)
    print(f"Combined map saved as {output_file}")

# Example usage
csv_file_path = 'locations.csv'
process_csv(csv_file_path)
