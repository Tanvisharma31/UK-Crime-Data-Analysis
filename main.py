import asyncio
import aiohttp
import asyncpg
import pandas as pd
from tqdm import tqdm
import configparser
import time

config = configparser.ConfigParser()
config.read('config.ini')

host = config['Postgres']['host']
db = config['Postgres']['db']
user = config['Postgres']['user']
password = config['Postgres']['password']
df = pd.read_csv("data/london_boroughs_borders.csv")

# Constants
MAX_CONCURRENT_REQUESTS = 3  # Maximum concurrent requests
BATCH_SIZE = 15  # Number of rows to process in each batch
MAX_REQUESTS_PER_SECOND = 15  # Max requests allowed per second

# Rate limiter
class RateLimiter:
    def __init__(self, max_requests_per_second):
        self.rate = 1.0 / max_requests_per_second
        self.last_request_time = 0

    async def wait(self):
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        if elapsed < self.rate:
            await asyncio.sleep(self.rate - elapsed)
        self.last_request_time = time.time()

async def get_id_from_table(pool) -> list[int]:
    """ Returns a list of ids present in the database """
    async with pool.acquire() as connection:
        sql = """SELECT id FROM table_name""" #TODO: Change table_name to your table name
        rows = await connection.fetch(sql)
        return [row['id'] for row in rows]

async def fetch_crime_data(session, row, limiter):
    """Returns a list of dicts of crime data"""
    await limiter.wait()  # Rate limiting
    url = f"https://data.police.uk/api/crimes-street/all-crime?date=2024-06&lat={row['latitude']}&lng={row['longitude']}"
    
    for attempt in range(5):  # Retry up to 5 times
        try:
            async with session.get(url, timeout=10) as response:  # Set a timeout
                if response.status == 200:
                    return await response.json()
                elif response.status == 429:  # Too Many Requests
                    wait_time = 2 ** attempt  # Exponential backoff
                    print(f"Rate limited. Waiting for {wait_time} seconds.")
                    await asyncio.sleep(wait_time)
                else:
                    response_text = await response.text()
                    print(f"Error {response.status}: {response_text}")
                    return []  # Return empty list if other error occurs

        except aiohttp.ClientPayloadError as e:
            print(f"Payload error: {e}. Retrying...")
            await asyncio.sleep(2 ** attempt)  # Exponential backoff
        except asyncio.TimeoutError:
            print("Request timed out. Retrying...")
            await asyncio.sleep(2 ** attempt)  # Exponential backoff
        except Exception as e:
            print(f"Error fetching data: {e}")
            return []  # Return empty list on other errors

    return []  # Return empty list after retries

async def process_batch(batch, pool, limiter):
    """Gathers data from api according to the batch number and returns a single dataframe"""
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_crime_data(session, row, limiter) for row in batch]
        results = await asyncio.gather(*tasks)

        combined_df = pd.DataFrame()
        for row, data in zip(batch, results):
            df2 = pd.DataFrame(data=data)
            df2['borough'] = row['borough']
            if not df2.empty:
                combined_df = pd.concat([combined_df, df2], ignore_index=True)

        return combined_df

async def upload_data(df, pool):
    """Uploads processed data into the database"""
    if df.empty:
        return
    
    for attempt in range(3):  # Retry on deadlock
        try:
            async with pool.acquire() as connection:
                await connection.executemany(
                    """
                    INSERT INTO table_name (category, location_type, location_latitude, 
                                                        location_longitude, context, outcome_status, 
                                                        persistent_id, id, location_subtype, 
                                                        month, year, borough)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    ON CONFLICT (id) DO NOTHING
                    """, #TODO : Change table_name to your table name
                    df.to_records(index=False)
                )
            break  # Exit loop if successful
        except asyncpg.exceptions.DeadlockDetectedError:
            wait_time = 2 ** attempt  # Exponential backoff
            print(f"Deadlock detected. Retrying after {wait_time} seconds.")
            await asyncio.sleep(wait_time)
        except Exception as e:
            print(f"Error inserting data: {e}")
            break  # Exit loop on other errors

async def process_data(df, pool, limiter):
    """Base function that fetches, processes and uploads data to the DB"""
    list_of_ids = await get_id_from_table(pool)

    for i in tqdm(range(0, len(df), BATCH_SIZE)):
        batch = df.iloc[i:i + BATCH_SIZE]
        batch_df = await process_batch(batch.to_dict(orient='records'), pool, limiter)
        
        if not batch_df.empty:
            for column in batch_df.columns:
                batch_df[column] = batch_df[column].fillna(value="None").replace("None", None)

            lat = [index['latitude'] for index in batch_df['location'] if index is not None]
            long = [index['longitude'] for index in batch_df['location'] if index is not None]
            os_category = [index2['category'] if index2 is not None else None for index2 in batch_df['outcome_status']]
            
            batch_df['location_latitude'] = lat
            batch_df['location_longitude'] = long
            batch_df['outcome_status'] = os_category
            batch_df['year'] = pd.DatetimeIndex(batch_df['month']).year
            batch_df['month'] = pd.DatetimeIndex(batch_df['month']).month
            batch_df.drop(columns=['location'], inplace=True)
            batch_df = batch_df[["category", "location_type", "location_latitude", "location_longitude", "context", "outcome_status", "persistent_id", "id", "location_subtype", "month", "year","borough"]]

            # Filter out already existing ids
            batch_df = batch_df[~batch_df['id'].isin(list_of_ids)]

            # Upload the batch to the database
            await upload_data(batch_df, pool)

async def main():
    pool = await asyncpg.create_pool(database=db, user=user, password=password, host=host)
    limiter = RateLimiter(MAX_REQUESTS_PER_SECOND)
    
    await process_data(df, pool, limiter)

    await pool.close()

# Run the main function
asyncio.run(main())