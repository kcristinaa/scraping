import requests
from bs4 import BeautifulSoup
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from geopy.exc import GeocoderInsufficientPrivileges, GeocoderTimedOut, GeocoderServiceError
from geopy.geocoders import Nominatim
from pymongo import MongoClient
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
try:
    load_dotenv()
except:
    pass

mongo_url = os.getenv('MONGO_URL')
mongo_port = int(os.getenv('MONGO_PORT'))
mongo_username = os.getenv('MONGO_USERNAME')
mongo_password = os.getenv('MONGO_PASSWORD')
mongo_auth_source = os.getenv('MONGO_AUTH_SOURCE')
mongo_auth_mechanism = os.getenv('MONGO_AUTH_MECHANISM')

def scrape_page(url, existing_orgs):
    try:
        response = requests.get(url)
        if response.status_code != 200:
            return None

        soup = BeautifulSoup(response.content, 'html.parser')
        articles = soup.select('.article-card.network-item')
        if not articles:
            return None

        page_data = []
        for article in articles:
            org_name = article.select_one('.article-content h2').text.strip()
            if org_name in existing_orgs:
                print(f"Skipping existing organization: {org_name}")
                continue

            location = article.select_one('footer h4').text
            active_in_elements = article.select('.term-list li a')
            active_in_data = ", ".join([elem.text for elem in active_in_elements]) if active_in_elements else "N/A"
            website_elem = article.select_one('.buttons .button.external')
            website_url = website_elem['href'] if website_elem else 'N/A'

            current_time = datetime.now()
            page_data.append({
                "Organization Name": org_name,
                "Location": location,
                "Active In": active_in_data,
                "Website": website_url,
                "Scraped At": current_time
            })

        return page_data
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None

def split_location(location):
    parts = location.split(', ')
    if len(parts) >= 2:
        return parts[0], parts[-1]
    return location, None

def get_coordinates(city, country):
    geolocator = Nominatim(user_agent="YourUniqueUserAgent")
    try:
        # First attempt with city and country
        location = geolocator.geocode(f'{city}, {country}') if city and country else geolocator.geocode(country)
        if location:
            return location.latitude, location.longitude

        # Fallback to country centroid if city-level geocoding fails
        if country:
            print(f"City-level geocoding failed for {city}, {country}. Falling back to country centroid.")
            location = geolocator.geocode(country)
            return (location.latitude, location.longitude) if location else (None, None)

        return None, None
    except GeocoderTimedOut:
        print("GeocoderTimedOut: Retrying...")
        try:
            # Retry once if timed out
            if city and country:
                location = geolocator.geocode(f'{city}, {country}')
            elif country:
                location = geolocator.geocode(country)
            else:
                return None, None

            if location:
                return location.latitude, location.longitude
            else:
                return None, None
        except Exception as e:
            print(f"Failed on retry: {e}")
            return None, None
    except GeocoderInsufficientPrivileges as e:
        print(f"Insufficient Privileges: {e}")
        return None, None
    except GeocoderServiceError as e:
        print(f"Service Error: {e}")
        return None, None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None, None

def save_to_mongo(data):
    # Create the client using environment variables
    client = MongoClient(mongo_url, int(mongo_port), username=mongo_username, password=mongo_password,
                        authSource=mongo_auth_source, authMechanism=mongo_auth_mechanism)
    db = client['trineflex']
    collection = db['rescoop']

    for record in data:
        org_name = record.get("Organization Name")

        existing_record = collection.find_one({"Organization Name": org_name})
        if existing_record:
            # Update existing record
            collection.update_one({"_id": existing_record["_id"]}, {"$set": record})
            print(f"Updated record for '{org_name}'.")
        else:
            # Insert new record
            collection.insert_one(record)
            print(f"Inserted new record for '{org_name}'.")

    client.close()

def main():
    # Load environment variables
    try:
        load_dotenv()
    except:
        pass

    base_link = "https://www.rescoop.eu/network"
    max_pages = 100
    batch_size = 50
    delay = 1
    data = []

    # Fetch existing organization names from the database
    client = MongoClient(mongo_url, int(mongo_port), username=mongo_username, password=mongo_password,
                         authSource=mongo_auth_source, authMechanism=mongo_auth_mechanism)
    db = client['trineflex']
    collection = db['rescoop']
    existing_orgs = set(collection.distinct("Organization Name"))
    client.close()

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_url = {executor.submit(scrape_page, base_link + "/p%d" % i, existing_orgs): i for i in range(1, max_pages + 1)}
        for future in as_completed(future_to_url):
            page_data = future.result()
            if page_data:
                data.extend(page_data)

    df = pd.DataFrame(data)
    df['City'], df['Country'] = zip(*df['Location'].apply(split_location))
    df.drop('Location', axis=1, inplace=True)
    df['Latitude'], df['Longitude'] = zip(*df.apply(lambda row: get_coordinates(row['City'], row['Country']), axis=1))

    save_to_mongo(df.to_dict(orient='records'))

if __name__ == "__main__":
    main()
