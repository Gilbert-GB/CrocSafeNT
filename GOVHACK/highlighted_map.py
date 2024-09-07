import pandas as pd  # Import pandas library for data manipulation
import folium  # Import folium library for creating interactive maps
from geopy.geocoders import Nominatim  # Import Nominatim for reverse geocoding
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable  # Import exceptions for handling geocoding errors
import time  # Import time module for implementing delays

def create_map():
    """
    Create a blank map object centered at latitude 0 and longitude 0, with a zoom level of 2.
    """
    # Create a blank map object with the specified initial location and zoom level
    return folium.Map(location=[0, 0], zoom_start=2)  # Center map at (0, 0) with zoom level 2

def add_circle_to_map(map_obj, lat, lon):
    """
    Add circles to the map at the specified latitude and longitude.
    
    Parameters:
    - map_obj: The folium map object to which circles will be added.
    - lat: Latitude of the location to add the circle.
    - lon: Longitude of the location to add the circle.
    """
    # Add a light red circle with a radius of 200 meters
    folium.Circle(
        location=[lat, lon],  # Location of the circle
        radius=200,  # Radius in meters
        color='red',  # Border color of the circle
        fill=True,  # Fill the circle with color
        fill_color='red',  # Fill color of the circle
        fill_opacity=0.3  # Opacity of the fill color
    ).add_to(map_obj)  # Add the circle to the map

    # Add a light orange circle with a radius of 500 meters
    folium.Circle(
        location=[lat, lon],  # Location of the circle
        radius=500,  # Radius in meters
        color='orange',  # Border color of the circle
        fill=True,  # Fill the circle with color
        fill_color='orange',  # Fill color of the circle
        fill_opacity=0.2  # Opacity of the fill color
    ).add_to(map_obj)  # Add the circle to the map

def reverse_geocode_with_retry(geolocator, coordinates, retries=3, timeout=10):
    """
    Perform reverse geocoding with retry mechanism in case of errors.
    
    Parameters:
    - geolocator: The geolocator object used for reverse geocoding.
    - coordinates: A tuple containing latitude and longitude to reverse geocode.
    - retries: Number of times to retry in case of failure.
    - timeout: Timeout for each geocoding request.
    
    Returns:
    - location: The location object containing address details or None if failed.
    """
    for attempt in range(retries):  # Retry up to 'retries' times
        try:
            # Attempt to get the location details
            location = geolocator.reverse(coordinates, timeout=timeout)
            return location  # Return the location details if successful
        except GeocoderTimedOut:
            # Handle timeout exception
            print(f"Geocoding service timed out on attempt {attempt + 1}. Retrying...")
            time.sleep(2 ** attempt)  # Wait before retrying (exponential backoff)
        except GeocoderUnavailable as e:
            # Handle geocoding service unavailability
            print(f"Geocoding service is unavailable: {e}")
            break  # Stop retrying if the service is unavailable
    return None  # Return None if all retries fail

def process_csv(file_path, batch_size=100):
    """
    Process a CSV file to create a map with circles representing locations.
    
    Parameters:
    - file_path: Path to the CSV file containing location data.
    - batch_size: Number of rows to process in each batch.
    """
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file_path)
    
    # Print column names in the DataFrame for debugging purposes
    print("Columns in the CSV file:", df.columns.tolist())
    
    # Print the first few rows of the DataFrame for debugging purposes
    print("First few rows of the DataFrame:")
    print(df.head())
    
    # Select every 5th row from the DataFrame to reduce the number of points
    df_sampled = df.iloc[::5].reset_index(drop=True)  # Downsample the DataFrame by taking every 5th row
    
    # Initialize the geolocator with a descriptive user-agent
    geolocator = Nominatim(user_agent="your_application_name_here")
    
    # Create a map object
    map_obj = create_map()
    
    # Process the data in batches to avoid overwhelming the geocoding service
    for start in range(0, len(df_sampled), batch_size):
        end = min(start + batch_size, len(df_sampled))  # Determine the end index for the current batch
        batch_df = df_sampled.iloc[start:end]  # Get the current batch of data
        
        # Process each row in the current batch
        for index, row in batch_df.iterrows():
            try:
                # Convert 'name' to string to avoid AttributeError
                name = str(row['name'])
                lat = row['latitude']  # Extract latitude
                lon = row['longitude']  # Extract longitude
                
                # Get the address from latitude and longitude with retry mechanism
                location = reverse_geocode_with_retry(geolocator, (lat, lon))
                address = location.address if location else "Address not found"  # Get address or default message
                
                # Add two circles (200m light red and 500m light orange) to the map
                add_circle_to_map(map_obj, lat, lon)
                
                # Print confirmation message
                print(f"Circles for {name} added to the map")
            except KeyError as e:
                # Handle missing column errors
                print(f"Column not found: {e}")
            except IndexError as e:
                # Handle index errors
                print(f"Index error: {e}. Skipping this row.")
        
        # Optional: sleep between batches to avoid hitting rate limits
        time.sleep(2)
    
    # Save the map to an HTML file
    output_file = 'highlighted_map.html'
    map_obj.save(output_file)  # Save the created map to an HTML file
    print(f"Highlighted map saved as {output_file}")  # Print confirmation message

# Example usage
csv_file_path = 'locations.csv'  # Path to the CSV file containing location data
process_csv(csv_file_path)  # Call the function to process the CSV file and create the map
