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


# Defining functions 
processes = {'PRO_01':'Temp','PRO_02':'RH', 'PRO_03':'PAR_max', 'PRO_04':'PAR_umol','PRO_05':'CO2'}

def data_processing(df):
  df = df[df['Quality'] == 192]
  df = df[['Time Stamp', 'Value']]
  df['datetime'] = df['Time Stamp'].str.split('.').str[0]
  df['datetime'] = df['datetime'].str.replace('T',' ')
  df['datetime'] = pd.to_datetime(df['datetime'])
  df.dropna(how='any', inplace=True)
  df_processed = df
  return df_processed

# Don't think the cols argument actaully works here, at least not when loading with StringIO like I am with drive. Might work on the version for my local computer. Likely should take out, it's easy enough to choose the columns I want, not really any more work than doing it as an argument
def data_from_date(date_folder, actual, time_offset = None, cols = None):
  if time_offset is None:
    time_offset = [pd.Timedelta(days=0), pd.Timedelta(days=0)]
  if not isinstance(actual, bool):
    raise ValueError('actual must be True or False')
  elif actual == True:
    string_id = 'Actual'
  elif actual == False:
    string_id = '_SP.'
  # gc_data = Path(f"/Users/sean/Documents/Sean/Lara Research/GC Data/Downloaded Data/{date_folder}")
  processed_files = set()
  list_df=[]
  date_folder_id = date_folder['id']
  for chamber_folder in drive.ListFile({'q':f"'{date_folder_id}' in parents and trashed=false"}).GetList():
    if 'other' not in chamber_folder['title']:
      # print(f"chamber folder {string_id}: {chamber_folder['title']}")
      st.write(f"chamber folder {string_id}: {chamber_folder['title']}")
      chamber_folder_id = chamber_folder['id']
      chamber = chamber_folder['title'].split('/')[-1].split('_')[0][-1]
      for process_folder in drive.ListFile({'q':f"'{chamber_folder_id}' in parents and trashed=false"}).GetList():
        process_folder_id = process_folder['id']
        for process in processes.keys():
          for csv_file in drive.ListFile({'q':f"'{process_folder_id}' in parents and trashed=false"}).GetList():
            if csv_file['title'].startswith('._'):
              continue  # skip macOS hidden files
            if csv_file['title'].split('.')[-1] == 'csv' and process in csv_file['title'] and string_id in csv_file['title']:
              if csv_file['title'] not in processed_files:
                processed_files.add(csv_file['title'])
                csv_id = csv_file['id']
                file = drive.CreateFile({'id': csv_id})
                csv_content = file.GetContentString()  # returns the raw CSV text
                csv_file = StringIO(csv_content)
                try:
                  try:
                    df = pd.read_csv(csv_file, usecols = cols)
                  except UnicodeDecodeError:
                    df = pd.read_csv(csv_file, encoding='latin1', engine = 'python', on_bad_lines='skip', usecols = cols)
                  df = data_processing(df)
                  df['Chamber'] = chamber
                  df['Process'] = processes[process]
                  df.reset_index(inplace=True)
                  list_df.append(df)
                except Exception as e:
                  print(f"Failed to read {csv_file['title']}: {e}")
              else:
                print(f"already processed {csv_file['title']}")
  df = pd.concat(list_df, axis=0)
  df.reset_index(inplace=True)
  df['minute'] = df['datetime'].dt.strftime('%m/%d/%Y %H')
  data = df.groupby(['minute', 'Chamber', 'Process']).agg({'Value':'mean'})
  data.reset_index(inplace=True)
  data = data.pivot(index=['minute', 'Chamber'], columns = 'Process', values = 'Value')
  data.reset_index(inplace=True)
  data.drop(columns = 'PAR_umol', inplace=True)
  data.rename(columns={'PAR_max':'PAR'}, inplace=True)
  data['minute'] = pd.to_datetime(data['minute'])
  data.loc[data['Chamber'] == 'A', 'minute'] = (data.loc[data['Chamber'] == 'A', 'minute'] + time_offset[0])
  data.loc[data['Chamber'] == 'B', 'minute'] = (data.loc[data['Chamber'] == 'B', 'minute'] + time_offset[1])
  data.dropna(how='any', inplace=True)
  return data

def read_drive_id(ID, cols = None):
  file = drive.CreateFile({'id': csv_id})
  csv_content = file.GetContentString()  # returns the raw CSV text
  csv_file = StringIO(csv_content)
  try:
    df = pd.read_csv(csv_file, usecols = cols)
  except UnicodeDecodeError:
    df = pd.read_csv(csv_file, encoding='latin1', engine = 'python', on_bad_lines='skip', usecols = cols)
  return df
    
offset_dict = {'20250509_Chamber_Data': [pd.Timedelta(days=30), pd.Timedelta(days=1)], '20250519_Chamber_Data': [pd.Timedelta(days=0), pd.Timedelta(days=-30)], '20250522_Chamber_Data':[pd.Timedelta(days=30), pd.Timedelta(days=0)], '20250602_Chamber_Data':[pd.Timedelta(days=40), pd.Timedelta(days=0)], '20250616_Chamber_Data': [pd.Timedelta(days=30), pd.Timedelta(days=0)], '20250620_Chamber_Data': [pd.Timedelta(days=30), pd.Timedelta(days=0)], '20250625_Chamber_Data' : [pd.Timedelta(days=-30), pd.Timedelta(days=0)], '20250627_Chamber_Data' : [pd.Timedelta(days=0), pd.Timedelta(days=0)], '20250701_Chamber_Data' : [pd.Timedelta(days=0), pd.Timedelta(days=0)], '20250702_Chamber_Data' : [pd.Timedelta(days=0), pd.Timedelta(days=0)], '20250703_Chamber_Data' : [pd.Timedelta(days=0), pd.Timedelta(days=0)], '20250707_Chamber_Data' : [pd.Timedelta(days=0), pd.Timedelta(days=0)], '20250714_Chamber_Data' : [pd.Timedelta(days=0), pd.Timedelta(days=0)], '20250721_Chamber_Data' : [pd.Timedelta(days=0), pd.Timedelta(days=0)], '20250725_Chamber_Data' : [pd.Timedelta(days=0), pd.Timedelta(days=0)], '20250804_Chamber_Data' : [pd.Timedelta(days=0), pd.Timedelta(days=0)], '20250806_Chamber_Data' : [pd.Timedelta(days=0), pd.Timedelta(days=0)], '20250808_Chamber_Data' : [pd.Timedelta(days=0), pd.Timedelta(days=0)]}
  
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

# # Below works on local computer. Above works on streamlit
# # Setting up access to google drive
# SERVICE_ACCOUNT_JSON = "/Users/sean/Documents/Sean/Lara Research/GC Data/operating-pod-469720-b9-214b1ebc73b3.json"
# with open(SERVICE_ACCOUNT_JSON) as f:
#     sa_info = json.load(f)
# client_email = sa_info["client_email"]
# gauth = GoogleAuth()
# gauth.settings = {
#     'client_config_backend': 'service',
#     'service_config': {
#         'client_json_file_path': SERVICE_ACCOUNT_JSON,
#         'client_user_email': client_email
#     },
#     'oauth_scope': ['https://www.googleapis.com/auth/drive']
# }
# gauth.ServiceAuth()
# drive = GoogleDrive(gauth)

# Looking through folder with processed data and finding that most recent 
processed_folder_id = '11x8zo1ZQYU_MuFh2A36f4TmGYaojEnpZ'
# processed_folder_id = '1v8Yu0IudRp_DiES99hVPMuF-Ma5lt5Zt'
search_term = 'gc_data_processed'
if processed_folder_id:
    query = f"'{processed_folder_id}' in parents and title contains '{search_term}' and trashed=false"
else:
    query = f"title contains '{search_term}' and trashed=false"
file_list = drive.ListFile({'q': query}).GetList()
date_dict = {}
for file in file_list:
  match = re.search(r'\d{1,2}[A-Za-z]{3}\d{2}', file['title'])
  if match:
    date_str = match.group(0)
    date = pd.to_datetime(date_str, format='%d%b%y')
    date_dict[date] = file
  else:
    st.write(f"No valid date found in {file['title']}")
  # date = file['title'].split('_')[-1].split('.')[0]
  # date = pd.to_datetime(date, format = '%d%b%y')
  # date_dict[date] = file
last_processing_date = max(date_dict.keys())
csv_id = date_dict[last_processing_date]['id']
columns = ['minute', 'Chamber', 'actual_sp','Temp', 'RH', 'PAR', 'CO2']
data_old = read_drive_id(csv_id, cols = columns)
last_processing_time = data_old['minute'].max()

# Looking through "Chamber Data folder and accessing new uploads since last process date
general_folder_id = "11Cdt-JEEeNaDLNdFj002mWmt5BgnHBrO"
# general_folder_id = "1BMnQR0UQ_Re7iSOHdFJ0jPHTRkVTchP7"
file_list = drive.ListFile({'q': f"'{general_folder_id}' in parents and trashed=false"}).GetList()
date_list = []
file_dict = {}
for file in file_list:
  try: 
    date = pd.to_datetime(file['title'].split('_')[0])
    date_list.append(date)
    if date > last_processing_date:
      file_dict[date] = file
  except Exception as e:
    file_name = file['title']
    print(f"failed to review {file_name}: {e}")
current_date = max(date_list).strftime(format = '%d%b%y')

# Adding as-yet unprocessed data to the primary dataset
data = data_old
data.loc[data['Temp'] > 36, 'Temp'] /= 10

for date in sorted(file_dict.keys()):
  st.write(date)
  if date in offset_dict.keys():
    time_offset = offset_dict[date]
  else:
    time_offset = [pd.Timedelta(days=0), pd.Timedelta(days=0)]
  data_actual_new = data_from_date(file_dict[date], actual=True, time_offset = time_offset)
  data_actual_new['actual_sp'] = 'actual'
  data_actual_new = data_actual_new[columns]
  data_sp_new = data_from_date(file_dict[date], actual=False, time_offset=time_offset)
  data_sp_new['actual_sp']='sp'
  data_sp_new = data_sp_new[columns]
  data_new = pd.concat([data_actual_new, data_sp_new])
  data_new = data_new[data_new['minute'] > last_processing_time]
  data_new.loc[data_new['Temp'] > 36, 'Temp'] /= 10
  data = pd.concat([data_new, data])
  # With data downloaded individually, the above cause some duplicate rows possibly, although it doesn't really make sense to me why
  # data.drop_duplicates(subset=['minute', 'Chamber', 'actual_sp', 'CO2'])

data_to_download = data.copy()


# Check to see if there is already an up-to-date processed file, and if not save new processed file
search_name = f"gc_data_processed_{current_date}"

duplicate_check = drive.ListFile({'q':f"'{processed_folder_id}' in parents and title contains '{search_name}' and trashed=false"}).GetList()
if not duplicate_check:
  st.write('There is an updated file!')
else:
  file_name = duplicate_check[0]['title']
  st.write('No new data')
csv_buffer = StringIO()
data_to_download.to_csv(csv_buffer, index=False)
csv_bytes = csv_buffer.getvalue().encode('utf-8')  # convert to bytes
st.download_button(
    label=f"Download {file_name}",
    data=csv_bytes,
    file_name=file_name,
    mime='text/csv'
)

# Changing definitions of data so compatible with graphing code I copied and pasted here
data['minute'] = pd.to_datetime(data['minute'])
variables = ['CO2', 'Temp', 'RH', 'PAR']
for var in variables:
  data[var] = pd.to_numeric(data[var])
data.dropna(how='any', inplace=True)
data = data.sort_values('minute')
min_date = data['minute'].min().to_pydatetime()
max_date = data['minute'].max().to_pydatetime()
# data['Temp']=data['Temp']/10

def chamber_actual_check(chamber=None, actual=None):
  co2_treatment = None
  actual_sp = None
  if chamber is not None:
    if chamber == 'A':
      co2_treatment = 'HiC'
    elif chamber == 'B':
      co2_treatment = 'LowC'
    else:
      st.error('Please select a chamber: A or B')
  if actual is not None:
    if actual == True:
      actual_sp = 'Actual'
    elif actual == False:
      actual_sp = 'Set Point'
    else:
      st.error('Please specify if you want to graph actual or setpoint values')
  return [co2_treatment, actual_sp]

units = {'CO2':'ppm', 'Temp':'degrees C', 'RH':'%', 'PAR':'umol/mol'}
def plotly_graph(data1, data2, var1, var2, colors=['blue', 'red'], axis_labels = None, legend_labels = None, title=None, x_range=None, y_range1=None, y_range2=None, units=units, key=None):
  if axis_labels is None:
    axis_labels = [f'{var1} {(units[var1])}', f'{var2} ({units[var2]})']
  if legend_labels is None:
    legend_labels = [var1, var2]
  if var1 == var2 and y_range1 is None and y_range2 is None:
    y_min = min(data1[var].min(), data2[var].min())
    y_max = max(data1[var].max(), data2[var].max())
    y_range1 = [y_min, y_max]
    y_range2 = [y_min, y_max]
  fig = make_subplots(specs=[[{"secondary_y": True}]])
  fig.add_trace(go.Scatter(x=data1['minute'], y=data1[var1], name=legend_labels[0], mode='lines', line=dict(color = colors[0])),secondary_y=False)
  fig.add_trace(go.Scatter(x=data2['minute'], y=data2[var2], name=legend_labels[1], mode='lines', line=dict(color = colors[1])),secondary_y=True)
  fig.update_xaxes(title_text="Time", range=x_range)
  fig.update_yaxes(title_text=axis_labels[0], range = y_range1, secondary_y=False)
  fig.update_yaxes(title_text=axis_labels[1], range = y_range2, secondary_y=True)
  fig.update_layout(title=title)
  fig.update_layout(title_font_size=26, xaxis_title_font_size=20, yaxis_title_font_size=20, tickfont=dict(size=20)
  # fig.update_xaxes(title_font=dict(size=18), tickfont=dict(size=14))
  if var1 == var2 and df1.equals(df2):
    fig.data = tuple(list(fig.data)[:-1])  # removes last trace
    fig.update_traces(line=dict(color='red'))
  st.plotly_chart(fig, use_container_width=True, key=key)

# Fully interavtive graph:
# data1_settings = st.multiselect(label='Relevant data for first line of graph: ', options = data1_options, default = ['A', 'actual'], key='data1_multiselect')
# data2_settings = st.multiselect(label='Relevant data for second line of graph: ', options = data2_options, default = ['B','actual'], key='data2_multiselect')

data1_chamber = st.radio(label='Select which chamber to graph as the first line: ', options = ['A', 'B'], index = 0, key='data1_chamber_radio')
data1_actual_sp = st.radio(label = 'Select whether to graph actual or set point as the first line: ', options = ['actual', 'sp'], index = 0, key='data1_actual_sp_radio')
data1_var = st.radio(label = 'Select which variable to graph as the first line: ', options=['CO2', 'RH', 'PAR', 'Temp'], index = 0, key='data1_var_radio')
data2_chamber = st.radio(label='Select which chamber to graph as the second line: ', options = ['A', 'B'], index=1, key='data2_chamber_radio')
data2_actual_sp = st.radio(label = 'Select whether to graph actual or set point as the second line: ', options = ['actual', 'sp'], index = 0, key='data2_actual_sp_radio')
data2_var = st.radio(label = 'Select which variable to graph as the second line: ', options=['CO2', 'RH', 'PAR', 'Temp'], index = 0, key='data2_var_radio')

df1 = data[data['Chamber'] == data1_chamber]
df1 = df1[df1['actual_sp'] == data1_actual_sp]
df2 = data[data['Chamber'] == data2_chamber]
df2 = df2[df2['actual_sp'] == data2_actual_sp]
if data1_chamber == 'A':
  co2_treatment_1 = 'High CO2'
elif data1_chamber == 'B':
  co2_treatment_1 = 'Low CO2'
if data1_actual_sp == 'actual':
  actual_1 = 'Actual'
elif data1_actual_sp == 'sp':
  actual_1 = 'Set Point'
if data2_chamber == 'A':
  co2_treatment_2 = 'High CO2'
elif data2_chamber == 'B':
  co2_treatment_2 = 'Low CO2'
if data2_actual_sp == 'actual':
  actual_2 = 'Actual'
elif data2_actual_sp == 'sp':
  actual_2 = 'Set Point'
if co2_treatment_1 == co2_treatment_2:
  if data1_var == data2_var:
    title = f'{data1_var} in {co2_treatment_1} Chamber'
    legend_labels = ['', '']
  else:
    title = f'{data1_var} and {data2_var} in {co2_treatment_1} Chamber'
    legend_labels = [f'{data1_var}', f'{data2_var}']
else:
  title = 'Interactive Graph'
  legend_labels = [f'{actual_1} {data1_var} in {co2_treatment_1} Chamber', f'{actual_2} {data2_var} in {co2_treatment_2} Chamber']
plotly_graph(df1, df2, data1_var, data2_var, legend_labels=legend_labels, title = title, key='interactive_graph')


def graph_plotly_var_par(df, chamber, actual, var1, var2='PAR', x_range=None, units = units, key=None):
  chamber_actual= chamber_actual_check(chamber, actual)
  df = df[(df['Chamber'] == chamber) & (df['actual_sp'] == ('actual' if actual else 'sp'))]
  title = f'{chamber_actual[1]} {var1} and {var2} in {chamber_actual[0]} Chamber'
  plotly_graph(df, df, var1, var2, x_range=x_range, units=units, title=title, key=key)

# Graphing variables with PAR
variables = ['CO2', 'RH', 'Temp']
for var in variables:
  for chamber in ['A','B']:
    graph_plotly_var_par(data, chamber, True, var, x_range = [min_date, max_date], key=f'{var}_with_PAR_chamber={chamber}')


df_actual = data[data['actual_sp'] == 'actual']
df_actual_a = df_actual[df_actual['Chamber']=='A']
df_actual_b = df_actual[df_actual['Chamber'] == 'B']
# df_sp = data[data['actual_sp'] == 'sp']
# colors = ['#ff9999', '#9999ff']
colors=['red', 'blue']
plotly_graph(df_actual_a, df_actual_a, 'PAR', 'CO2', colors=colors, axis_labels = None, legend_labels = None, title='CO2 and PAR in High CO2 Chamber', x_range=None, y_range1=[0,1350], y_range2=[0,1350], units=units, key=None)
plotly_graph(df_actual_b, df_actual_b, 'PAR', 'CO2', colors=colors, axis_labels = None, legend_labels = None, title='CO2 and PAR in Low CO2 Chamber', x_range=None, y_range1=[0,1350], y_range2=[0,1350], units=units, key=None)

colors=['orange', 'green']
plotly_graph(df_actual_a, df_actual_a, 'RH', 'Temp', colors=colors, axis_labels = None, legend_labels = None, title='Temp and RH in High CO2 Chamber', x_range=None, y_range1=[0,100], y_range2=[0,40], units=units, key=None)
plotly_graph(df_actual_b, df_actual_b, 'RH', 'Temp', colors=colors, axis_labels = None, legend_labels = None, title='Temp and RH in Low CO2 Chamber', x_range=None, y_range1=[0,100], y_range2=[0,40], units=units, key=None)

# SP vs actual (comparing setpoint and actual variables for each chamber)
def graph_actual_sp(df, var, chamber, colors = ['blue', 'orange'], x_range=None, key=None):
  chamber_name = chamber_actual_check(chamber, True)[0]
  df = df[df['Chamber'] == chamber]
  data1=df[df['actual_sp'] == 'actual']
  data2 = df[df['actual_sp'] == 'sp']
  title = f'Actual and Set Point of {var} in {chamber_name} Chamber'
  plotly_graph(data1, data2, var, var, colors = colors, legend_labels = ['actual', 'setpoint'], title=title, x_range=x_range, key=key)

variables = ['CO2', 'RH', 'Temp', 'PAR']
for var in variables:
  for chamber in ['A', 'B']:
    graph_actual_sp(data, var, chamber, key=f'{var}_actual_sp_comparison_chamber={chamber}')

# SP and actual (comparing side by side the setpoint and the actual measurements)

def graph_chamber(df, var, is_actual, colors = ['purple', 'green'], x_range=None, key=None):
  # st.write('is_actual', is_actual)
  # actual = chamber_actual_check(actual = is_actual)[-1]
  # st.write('actual', actual)
  df = df[df['actual_sp'] == ('actual' if is_actual else 'sp')]
  df_a = df[df['Chamber'] == 'A']
  df_b = df[df['Chamber'] == 'B']
  actual_title = ('Actual' if is_actual else 'Set Point')
  title = f'{var} in Both Chambers, {actual_title}'
  plotly_graph(df_a, df_b, var, var, colors=colors, legend_labels = ['A', 'B'], x_range=x_range, title=title, key=key)

variables = ['CO2', 'RH', 'Temp', 'PAR']
for var in variables:
  for is_actual in [True, False]:
    graph_chamber(data, var, is_actual, key=f'{var}_chamber_comparison_actual={is_actual}')
 
