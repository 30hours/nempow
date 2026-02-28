import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import time
import logging
import zipfile
import io
from apscheduler.schedulers.blocking import BlockingScheduler

def db_init():

  conn_params = {
    "dbname": "db",
    "user": "admin",
    "password": "password",
    "host": "db",
    "port": "5432"
  }

  # connect to db loop
  while True:
    try:
      conn = psycopg2.connect(**conn_params)
      logging.info('Connection to database successful')
      break
    except:
      logging.warning('Waiting for database...')
      time.sleep(2)
      
  try:
    cur = conn.cursor()

    # create the power table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS power (
      id SERIAL PRIMARY KEY,
      timestamp_iso TIMESTAMP NOT NULL,
      plant_id TEXT NOT NULL,
      power_mw NUMERIC(10, 2)
    );
    """
    cur.execute(create_table_query)
    
    # create the plant table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS plant (
      id TEXT PRIMARY KEY,
      name TEXT,
      type TEXT,
      state TEXT,
      capacity NUMERIC(10, 2)
    );
    """
    cur.execute(create_table_query)

    # close
    conn.commit()
    cur.close()
    
    return conn

  except Exception as e:
    logging.error(f"An error occurred: {e}")
    
def get_plant_info(file):
  
  """
  Read in the NEM Generator Information file.
  """
  
  df = pd.read_excel(file, sheet_name='Generator Information', header=3)

  # filter to useful info
  df = df[df['Commitment Status'] == 'In Service']
  cols = ['Site Name', 'Region', 'DUID', 'Technology Type', 'Max Site Capacity (AC)']
  df = df[cols]
  
  # remove postfix '1' from state e.g. SA1 becomes SA
  df['Region'] = df['Region'].str[:-1]
  
  # there appear to be duplicate DUIDs for some units
  # group and sum capacity in this case
  # ANGAST1 is example with 30.8 and 15.4 MW capacity
  # corresponds to units 1-30 and 13-24
  # but MCKAY1 has 6 25MW generators and 6 entries 
  df = df.groupby('DUID').agg({
    'Site Name': 'first',
    'Technology Type': 'first',
    'Region': 'first',
    'Max Site Capacity (AC)': 'sum'
  }).reset_index()
  
  # drop any duds
  df = df.dropna(subset=['DUID'])
  
  return df
  
def populate_db_plant(df, conn):

  """
  Populate the plant table using the generator info dataframe.
  """

  try:
    cur = conn.cursor()

    # prepare data
    data_to_insert = [
      (row['DUID'], row['Site Name'], row['Technology Type'], row['Region'], row['Max Site Capacity (AC)'])
      for _, row in df.iterrows()
    ]

    # batch insert
    insert_query = """
    INSERT INTO plant (id, name, type, state, capacity)
    VALUES %s
    ON CONFLICT (id) DO UPDATE SET
      name = EXCLUDED.name,
      type = EXCLUDED.type,
      state = EXCLUDED.state,
      capacity = EXCLUDED.capacity;
    """

    execute_values(cur, insert_query, data_to_insert)

    # close
    conn.commit()
    cur.close()

    logging.info(f"Successfully populated 'plant' table with {len(data_to_insert)} rows")

  except Exception as e:
    logging.error(f"Failed to populate plant table: {e}")
    conn.rollback()
  
def get_manifest_live(url):
  
  """
  Get a list of files at a URL.
  """

  try:
    
    # request data
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    # parse HTML
    soup = BeautifulSoup(response.text, 'html.parser')
    manifest = []

    # find all links
    for link in soup.find_all('a'):
      file_name = link.get('href')
      
      # only interested in ZIP files
      # store ISO timestamp and filename
      if file_name and file_name.endswith('.zip'):
        full_url = urljoin(url, file_name)
        manifest.append({
          'timestamp': datetime.strptime(
            file_name.split('/')[-1].split('_')[2], 
            '%Y%m%d%H%M').isoformat(),
          'url': full_url
        })

    return manifest

  except Exception as e:
    logging.error(f"An error occurred: {e}")
    return []
  
def populate_db_live(manifest, conn):
  
  """
  Loop through the live manifest, download files and populate the db.
  """
  
  cur = conn.cursor()
  
  for entry in manifest:
    
    # skip if timestamp already in db
    cur.execute("SELECT 1 FROM power WHERE timestamp_iso = %s LIMIT 1", (entry['timestamp'],))
    exists = cur.fetchone()
    if exists:
      logging.info(f'Skip {entry['timestamp']}, already in database')
      continue
    
    # download data if not in db
    response = requests.get(entry['url'])
    if response.status_code == 200:
      
      # unzip
      with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        csv_filename = z.namelist()[0]
        
        with z.open(csv_filename) as f:
          # read csv into pandas
          df = pd.read_csv(f, header=1)
          
          # filter only the DUID and SCADAVALUE columns
          if 'DUID' in df.columns and 'SCADAVALUE' in df.columns:
            df_clean = df.dropna(subset=['DUID', 'SCADAVALUE'])
            
            # prepare data
            data_to_insert = [
              (entry['timestamp'], row['DUID'], row['SCADAVALUE']) 
              for _, row in df_clean.iterrows()
            ]

            # batch insert
            insert_query = """
            INSERT INTO power (timestamp_iso, plant_id, power_mw) 
            VALUES %s
            """
            execute_values(cur, insert_query, data_to_insert)
            conn.commit()
            logging.info(f"Inserted {len(data_to_insert)} rows for {entry['timestamp']}") 

    else:
        logging.info(f'Failed to download {entry['url']}')

def get_manifest_archive(url):

  """
  Get a list of archive daily ZIP files.
  """

  try:

    # request data
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    # parse HTML
    soup = BeautifulSoup(response.text, 'html.parser')
    manifest = []

    # find all links
    for link in soup.find_all('a'):
      file_name = link.get('href')

      # archive files are daily zips e.g. PUBLIC_DISPATCHSCADA_20250222.zip
      if file_name and file_name.endswith('.zip'):
        full_url = urljoin(url, file_name)
        
        # isolate filename from path
        # handle /REPORTS/ARCHIVE/Dispatch_SCADA/PUBLIC_DISPATCHSCADA_20250222.zip
        just_file = file_name.split('/')[-1]
        
        try:
          # parse date from filename (index 2)
          # parts: ['PUBLIC', 'DISPATCHSCADA', '20250222.zip']
          parts = just_file.split('_')
          date_str = parts[2].replace('.zip', '')
          file_date = datetime.strptime(date_str, '%Y%m%d')
        except Exception as e:
          logging.warning(f"Could not parse archive date from {just_file}: {e}")
          continue

        manifest.append({
          'url': full_url,
          'file_name': just_file,
          'date': file_date
        })

    # sort newest first
    manifest.sort(key=lambda x: x['date'], reverse=True)

    return manifest

  except Exception as e:
    logging.error(f"An error occurred: {e}")
    return []

def populate_db_archive(manifest, conn):

  """
  Loop through daily archive ZIPs, extract nested 5-min ZIPs, and populate DB.
  """

  cur = conn.cursor()

  for entry in manifest:

    # check if day already processed
    # we check if any data exists for this specific day
    day_str = entry['date'].strftime('%Y-%m-%d')
    cur.execute("SELECT 1 FROM power WHERE timestamp_iso::text LIKE %s LIMIT 1", (f"{day_str}%",))
    if cur.fetchone():
      logging.info(f"Skip archive {entry['file_name']}, date {day_str} already in database")
      continue

    # download daily zip
    logging.info(f"Downloading archive: {entry['file_name']}")
    response = requests.get(entry['url'])
    
    if response.status_code == 200:

      # open top-level zip
      with zipfile.ZipFile(io.BytesIO(response.content)) as daily_zip:

        # get list of internal 5-minute zips
        sub_zips = sorted([f for f in daily_zip.namelist() if f.endswith('.zip')])
        logging.info(f"Unpacking {len(sub_zips)} intervals from {entry['file_name']}")

        for sub_zip_name in sub_zips:
          try:
            # extract timestamp from sub-zip filename
            # PUBLIC_DISPATCHSCADA_YYYYMMDDHHMM_ID.zip
            ts_raw = sub_zip_name.split('_')[2]
            ts_iso = datetime.strptime(ts_raw, '%Y%m%d%H%M').isoformat()
          except:
            continue

          # skip if interval already in db
          cur.execute("SELECT 1 FROM power WHERE timestamp_iso = %s LIMIT 1", (ts_iso,))
          if cur.fetchone():
            continue

          # extract the nested zip
          with daily_zip.open(sub_zip_name) as sub_zip_data:
            with zipfile.ZipFile(io.BytesIO(sub_zip_data.read())) as z:
              csv_filename = z.namelist()[0]

              with z.open(csv_filename) as f:
                # read csv into pandas
                df = pd.read_csv(f, header=1)

                if 'DUID' in df.columns and 'SCADAVALUE' in df.columns:
                  df_clean = df.dropna(subset=['DUID', 'SCADAVALUE'])
                  
                  data_to_insert = [
                    (ts_iso, row['DUID'], row['SCADAVALUE'])
                    for _, row in df_clean.iterrows()
                  ]

                  insert_query = "INSERT INTO power (timestamp_iso, plant_id, power_mw) VALUES %s"
                  execute_values(cur, insert_query, data_to_insert)
                  conn.commit()
        
        logging.info(f"Finished processing archive: {entry['file_name']}")

    else:
      logging.info(f"Failed to download archive {entry['url']}")

def run_live(url_live, conn):
  logging.info("Start run live processing of NEM data...")
  try:
    manifest_live = get_manifest_live(url_live)
    populate_db_live(manifest_live, conn)
    logging.info('Finished running live')
  except Exception as e:
    logging.error(f"Error occurred: {e}")
    
def run_archive(url_archive, conn):
  logging.info("Start archive processing...")
  try:
    manifest_archive = get_manifest_archive(url_archive)
    populate_db_archive(manifest_archive, conn)
    logging.info('Done archive')
  except Exception as e:
    logging.error(f"Error occurred: {e}")


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',)

# save generator info
file_generator = '/app/data/NEM Generation Information Jan 2026.xlsx'
plant = get_plant_info(file_generator)

# setup processing
conn = db_init()
populate_db_plant(plant, conn)
url_live = 'https://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/'
url_archive = 'https://www.nemweb.com.au/REPORTS/ARCHIVE/Dispatch_SCADA/'
scheduler = BlockingScheduler()

# run live processing on every 5th minute and now
scheduler.add_job(run_live, 'cron', minute='*/5', 
  args=[url_live, conn], next_run_time=datetime.now())

# run archive processing only once
scheduler.add_job(run_archive, 'date',
  args=[url_archive, conn], run_date=datetime.now())

scheduler.start()