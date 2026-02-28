import os
import io
import csv
import psycopg2
from psycopg2 import pool
from datetime import datetime
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import PlainTextResponse, FileResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
import matplotlib.dates as mdates
from matplotlib.figure import Figure
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',)

try:
    # database connection settings
    DB_PARAMS = {
      "dbname": "db",
      "user": "admin",
      "password": "password",
      "host": "db",
      "port": "5432"
    }
    db_pool = psycopg2.pool.ThreadedConnectionPool(
        1, 20, **DB_PARAMS
    )
except Exception as e:
    logging.error(f"Error creating connection pool: {e}")
    exit(1)

# setup app and rate limiting
app = FastAPI()
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

def get_db_conn():
  
  """
  Create a new connection to the database.
  """
  
  return db_pool.getconn()

def query_plant_data():
  
  """
  Query all plant information and return in CSV format.
  """
  
  conn = db_pool.getconn()
  cur = conn.cursor()

  # query all plants
  query = """
  SELECT id, name, type, state, capacity 
  FROM plant 
  ORDER BY id ASC;
  """
  
  try:
    
    # execute query
    cur.execute(query)
    rows = cur.fetchall()
    
    # format as CSV
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['id', 'name', 'type', 'state', 'capacity'])
    
    for row in rows:
      # map numeric to float for csv
      writer.writerow([row[0], row[1], row[2], row[3], float(row[4]) if row[4] else 0.0])
            
    cur.close()
    return output.getvalue()

  except Exception as e:
    logging.error(f"Database error: {e}")
    return None

  finally:
    if conn:
      db_pool.putconn(conn)

def query_power_data(plant_id, date_obj):
  
  """
  Query power data for a specific plant and date, returning CSV format.
  """
  
  conn = db_pool.getconn()
  cur = conn.cursor()

  # query data for the specific plant and day
  query = """
  SELECT timestamp_iso, power_mw 
  FROM power 
  WHERE plant_id = %s 
    AND timestamp_iso::date = %s
  ORDER BY timestamp_iso ASC;
  """
  
  try:
    
    # execute query
    cur.execute(query, (plant_id, date_obj))
    rows = cur.fetchall()
    
    # format as CSV
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['timestamp_iso', 'power_mw'])
    
    for row in rows:
      # format timestamp to ISO string
      writer.writerow([row[0].isoformat(), float(row[1])])
            
    cur.close()
    return output.getvalue()

  except Exception as e:
    logging.error(f"Database error: {e}")
    return None

  finally:
    if conn:
      db_pool.putconn(conn)
      
@app.get("/")
async def read_index(request: Request):
  return FileResponse('index.html')
      
@app.get("/api/plants", response_class=PlainTextResponse)
@limiter.limit("10/minute")
def get_plants(request: Request):

  """
  API endpoint to get the list of all generator plants in CSV format.
  """
  
  # fetch from db
  data = query_plant_data()
  
  if data is None:
    raise HTTPException(status_code=500, detail="Database error")
    
  return data

@app.get("/api/{plant_id}/today", response_class=PlainTextResponse)
@limiter.limit("10/minute")
def get_today(request: Request, plant_id: str):

  """
  API endpoint to get power data for the current day by calling get_by_date.
  """
  
  # generate todays date in YYYYMMDD format
  today_str = datetime.now().strftime("%Y%m%d")
  
  # call the main endpoint
  return get_by_date(request, plant_id, today_str)

@app.get("/api/{plant_id}/{date_str}", response_class=PlainTextResponse)
@limiter.limit("10/minute") 
def get_by_date(request: Request, plant_id: str, date_str: str):
  
  """
  API endpoint to get power data for a specific date (YYYYMMDD).
  """
  
  if len(plant_id) > 10:
    raise HTTPException(status_code=418, detail="Plant ID invalid (too long). Also I'm a teapot")
  
  try:
    # convert YYYYMMDD string to date object
    date_obj = datetime.strptime(date_str, "%Y%m%d").date()
  except ValueError:
    raise HTTPException(status_code=400, detail="Invalid date format. Use YYYYMMDD")
  
  # fetch from db
  data = query_power_data(plant_id, date_obj)
  
  if data is None:
    raise HTTPException(status_code=500, detail="Database error")
    
  return data

@app.get("/api/{plant_id}/{date_str}/plot", response_class=Response)
@limiter.limit("10/minute") 
def plot_by_date(request: Request, plant_id: str, date_str: str):
  
  """
  API endpoint to get plot data for a specific date (YYYYMMDD).
  """
  
  if len(plant_id) > 10:
    raise HTTPException(status_code=418, detail="Plant ID invalid (too long). Also I'm a teapot")
  
  try:
    # convert YYYYMMDD string to date object
    date_obj = datetime.strptime(date_str, "%Y%m%d").date()
  except ValueError:
    raise HTTPException(status_code=400, detail="Invalid date format. Use YYYYMMDD")
  
  # fetch from db
  data = query_power_data(plant_id, date_obj)
  
  if data is None:
    raise HTTPException(status_code=500, detail="Database error")
    
  # read into pandas (a little hacky)
  df = pd.read_csv(io.StringIO(data))
  if df.empty:
    raise HTTPException(status_code=404, detail="No data found for this date/plant")
  df['timestamp_iso'] = pd.to_datetime(df['timestamp_iso'])

  # need to use matplotlib Figure API to be thread safe
  fig = Figure(figsize=(16, 10))
  ax = fig.subplots()
  ax.plot(df['timestamp_iso'], df['power_mw'])
  ax.set_title(f'Power Output for {plant_id} on {date_obj.strftime("%Y-%m-%d")}')
  ax.set_xlabel(f'Timestamp on {date_obj.strftime("%Y-%m-%d")}')
  ax.set_ylabel('Power (MW)')
  ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
  fig.autofmt_xdate()
  ax.grid(True, linestyle='--', alpha=0.5)

  # save to buffer
  buf = io.BytesIO()
  fig.savefig(buf, format="png", bbox_inches='tight')
  buf.seek(0)
  
  return Response(content=buf.getvalue(), media_type="image/png")