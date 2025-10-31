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

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

import pandas as pd
import numpy as np
from scipy import stats
import itertools
import plotly.graph_objects as go

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

def read_drive_id(ID, cols = None):
  file = drive.CreateFile({'id': ID})
  file_title = file['title']
  csv_content = file.GetContentString()  # returns the raw CSV text
  csv_file = StringIO(csv_content)
  try:
    df = pd.read_csv(csv_file, usecols = cols)
  except UnicodeDecodeError:
    df = pd.read_csv(csv_file, encoding='latin1', engine = 'python', on_bad_lines='skip', usecols = cols)
  return [df, file_title]

drive = google_drive_access_streamlit()
id = '1w4P14LWMUymx6LsWB0rFhhNjowrBDksb'
[data, title] = read_drive_id(id)

  
match = re.search(r'\d{1,2}[A-Za-z]{3}\d{2}', title)
if match:
  download_date = match.group(0)
else:
  st.write(f"No valid date found in {title}")

st.write(f"Interactive plots from germination datasheet: {download_date} download")
  
# drive = google_drive_access_streamlit()
# id = '1w4P14LWMUymx6LsWB0rFhhNjowrBDksb'
# data = read_drive_id(id)

data.reset_index(inplace=True)
data.rename(columns={'Pot Row':'Row', 'Pot Column':'Column'}, inplace=True)
cols = [col for col in data.columns if 'Unnamed' not in col and 'index' not in col and 'Notes' not in col and not 'growth rate' in col]
data = data[cols]
data.dropna(subset=['Species'], inplace=True)
data['pot_id'] = data.apply(lambda row: f"{str(row['Chamber'])}{str(row['Bin']).split('.')[0]}{str(row['Row']).split('.')[0]}{str(row['Column']).split('.')[0]}", axis=1)

data.set_index('pot_id', inplace=True)

# Dropping columns of soil moisture where only a few were measured
data = data.drop(columns=[col for col in data.columns if '6/11/2025' in col and 'VWC' in col])



# VWC analysis over time

df=data.copy()
df.reset_index(inplace=True)

index_cols = ['pot_id', 'Chamber', 'Bin', 'Row', 'Column', 'Species', 'Watering Regime']
vwc_cols = [col for col in df.columns if 'VWC' in col]

cols = index_cols+vwc_cols

df = df[cols]


df_long = df.melt(id_vars = index_cols, value_vars = vwc_cols, var_name = 'variable_name', value_name = 'value')
df_long[['date', 'measurement_type']] = df_long['variable_name'].str.split('-', expand=True)
df_long[['date', 'measurement_type']] = df_long[['date', 'measurement_type']].apply(lambda x: x.str.strip())
df_long = df_long.drop(columns=['variable_name'])

index_cols = index_cols + ['date']

df_wide = df_long.pivot(index=index_cols, columns = 'measurement_type', values = 'value')
df_wide.reset_index(inplace=True)
df_wide.rename(columns = {'VWC (%) Handheld sensor':'vwc'}, inplace=True)
df_wide['vwc'] = pd.to_numeric(df_wide['vwc'], errors='coerce')
df_wide['date'] = pd.to_datetime(df_wide['date'])
df_wide.sort_values('date',inplace=True)


# grouping_cols is a list of treatments I want to group by. Don't include date column. confidence is what percent want the confidence interval to be. Default value is 0.95
def summarize(df, grouping_cols, confidence = None):
  if confidence is None:
    confidence = 0.95
  grouped = df.groupby(grouping_cols+['date'])['vwc']
  summary = grouped.agg(['mean', 'count', 'std', 'min', 'max'])
  summary['sem'] = summary['std'] / np.sqrt(summary['count'])
  summary['ci95'] = summary['sem'] * stats.t.ppf((1 + confidence) / 2, summary['count'] - 1)
  summary['ci_upper'] = summary['mean'] + summary['ci95']
  summary['ci_lower'] = summary['mean'] - summary['ci95']
  summary.reset_index(inplace=True)
  return summary


# restricting analysis to oak species
df_oaks = df_wide[(df_wide['Species'] == 'quch') | (df_wide['Species'] == 'quwi')].copy()

# del df_wide

# renaming chambers for clarity of CO2 treatment
df_oaks.loc[:,'Chamber'] = df_oaks.loc[:,'Chamber'].replace({'A':'High CO2', 'B':'Low CO2'})

treatments = ['Watering Regime', 'Chamber', 'Species']

treatment_combinations = [list(t) for r in range(1, len(treatments)+1) for t in itertools.combinations(treatments, r)]

grouping_dict = {}
summary_dict = {}
for grouping_cols in treatment_combinations:
  treatment_combination_name = ' and '.join(grouping_cols)
  grouping_dict[treatment_combination_name] = grouping_cols
  summary = summarize(df_oaks, grouping_cols)
  summary_dict[treatment_combination_name] = summary


def plotly_go_graphing(summary, grouping_cols, title):
  # Create a base figure
  fig = go.Figure()
  # Loop over groups (e.g., by Watering Regime)
  for name, group in summary.groupby(grouping_cols):
    legend_group_name = str(name)
    fig.add_trace(go.Scatter(
      x=group['date'], 
      y=group['mean'], 
      mode='lines', 
      name=f'{name} mean', 
      # error_y=dict(
      #   type='data',          # error bars are in data units
      #   array=group['ci95'],  # distance above each point
      #   visible=True),
      # legendgroup=legend_group_name
      # ))
    error_y=dict(
      type='data',
      symmetric=False,                  # asymmetric whiskers
      array=group['max'] - group['mean'],      # distance from mean to max
      arrayminus=group['mean'] - group['min'], # distance from mean to min
      visible=True
      ))
    # Add shaded confidence interval
    fig.add_trace(go.Scatter(
      x=pd.concat([group['date'], group['date'][::-1]]),
      y=pd.concat([group['ci_upper'], group['ci_lower'][::-1]]),
      fill='toself',
      fillcolor='rgba(0,100,80,0.2)',
      line=dict(color='rgba(255,255,255,0)'),
      hoverinfo="skip",
      showlegend=False, 
      legendgroup=legend_group_name
      ))
    # fig.add_trace(go.Scatter(
    #   x=pd.concat([group["date"], group["date"][::-1]]),
    #   y=pd.concat([group["min"], group["max"][::-1]]),
    #   mode="lines",
    #   line=dict(color="gray", width=2, dash="dot"),
    #   name="Whiskers 1",
    #   legendgroup=legend_group_name,
    #   showlegend=False,
    #   ))
    # for i, row in group.iterrows():
    #   fig.add_trace(go.Scatter(
    #     x=[row['date'], row['date']],
    #     y=[row['min'], row['max']],
    #     mode='lines',
    #     line=dict(color='rgba(255,255,255,0)', width=1.5),
    #     showlegend=False,
    #     hoverinfo='skip',
    #     legendgroup=legend_group_name
    #     ))    
    fig.update_layout(
      title=title,
      xaxis_title="Date",
      yaxis_title="Mean VWC (%)",
      template="plotly_white"
      )
  return fig  

figures = []
for treatment_combo in summary_dict.keys():
  title = f"VWC (%) with 95% Confidence Intervals: Grouped by {treatment_combo}"
  grouping_cols = grouping_dict[treatment_combo]
  summary = summary_dict[treatment_combo]
  fig = plotly_go_graphing(summary, grouping_cols, title)
  figures.append(fig)


for fig in figures:
  st.plotly_chart(fig, use_container_width=True)

# figures[0]
# figures[1]
# figures[2]
# figures[3]
# figures[4]
# figures[5]
# figures[6]

