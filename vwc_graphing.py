from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import json
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
# import requests
from io import StringIO
from datetime import date
import streamlit as st
import plotly.express as px 
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import re

def google_drive_access_streamlit():
  sa_json_str = st.secrets["SERVICE_ACCOUNT_JSON"]
  client_email = json.loads(sa_json_str)["client_email"]
  gauth = GoogleAuth()
  gauth.settings = {
    'client_config_backend': 'service',
    'service_config': {
        'client_json_file_path': None,
        'client_json': sa_json_str,
        'client_user_email': client_email
    },
    'oauth_scope': ['https://www.googleapis.com/auth/drive']
  }
  gauth.ServiceAuth()
  drive = GoogleDrive(gauth)
  return drive

drive = google_drive_access_streamlit()

def read_drive_id(ID, cols = None):
  file = drive.CreateFile({'id': csv_id})
  csv_content = file.GetContentString()  # returns the raw CSV text
  csv_file = StringIO(csv_content)
  try:
    df = pd.read_csv(csv_file, usecols = cols)
  except UnicodeDecodeError:
    df = pd.read_csv(csv_file, encoding='latin1', engine = 'python', on_bad_lines='skip', usecols = cols)
  return df

id = '1Nwj8FKqPq5ILa6c0VlYL98QIP-7FNmAi8N8CPK39XjM'

data = read_drive_id(id)
st.write(data.head())
