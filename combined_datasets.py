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
      print(f"chamber folder {string_id}: {chamber_folder['title']}")
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

# with open(SERVICE_ACCOUNT_JSON) as f:
#     sa_info = json.load(f)
# client_email = sa_info["client_email"]
gauth = GoogleAuth()

# Fully define settings dictionary
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

# Looking through folder with processed data and finding that most recent date
processed_folder_id = '11x8zo1ZQYU_MuFh2A36f4TmGYaojEnpZ'
search_term = 'gc_data_processed'
if processed_folder_id:
    query = f"'{processed_folder_id}' in parents and title contains '{search_term}' and trashed=false"
else:
    query = f"title contains '{search_term}' and trashed=false"
file_list = drive.ListFile({'q': query}).GetList()
date_dict = {}
for file in file_list:
  date = file['title'].split('_')[-1].split('.')[0]
  date = pd.to_datetime(date, format = '%d%b%y')
  date_dict[date] = file
last_processing_date = max(date_dict.keys())
csv_id = date_dict[last_processing_date]['id']
columns = ['minute', 'Temp', 'RH', 'PAR', 'CO2']
data_old = read_drive_id(csv_id, cols = columns)
last_processing_time = data_old['minute'].max()

# Looking through "Chamber Data folder and accessing new uploads since last process date
general_folder_id = "11Cdt-JEEeNaDLNdFj002mWmt5BgnHBrO"
file_list = drive.ListFile({'q': f"'{general_folder_id}' in parents and trashed=false"}).GetList()
date_list = []
file_dict = {}
for file in file_list:
  try: 
    date = pd.to_datetime(file['title'].split('_')[0])
    if date > last_processing_date:
      date_list.append(date)
      file_dict[date] = file
  except Exception as e:
    file_name = file['title']
    print(f"failed to review {file_name}: {e}")

# Adding as-yet unprocessed data to the primary dataset
data = data_old
for date in sorted(file_dict.keys()):
  if date in offset_dict.keys():
    time_offset = offset_dict[date]
  else:
    time_offset = [pd.Timedelta(days=0), pd.Timedelta(days=0)]
  data_actual_new = data_from_date(file_dict[date], actual=True, time_offset = time_offset)
  data_actual_new = data_actual_new[columns]
  data_sp_new = data_from_date(file_dict[date], actual=False, time_offset=time_offset)
  data_sp_new = data_sp_new[columns]
  data_new = pd.concat([data_actual_new, data_sp_new])
  data_new = data_new[data_new['minute'] > last_processing_time]
  data = pd.concat([data_new, data])


# Check to see if there is already an up-to-date processed file, and if not save new processed file
current_date = date.today().strftime('%d%b%y')
file_name = f"gc_data_processed_{current_date}.csv"

duplicate_check = drive.ListFile({'q':f"'{processed_folder_id}' in parents and title contains '{file_name}' and trashed=false"}).GetList()
if not duplicate_check:
  st.write('There is an updated file!')
  st.write('No new data')
csv_buffer = StringIO()
data.to_csv(csv_buffer, index=False)
csv_bytes = csv_buffer.getvalue().encode('utf-8')  # convert to bytes
st.download_button(
    label="Download Combined Dataset",
    data=csv_bytes,
    file_name=file_name,
    mime='text/csv'
)

# # This last piece of uploading new dataset to google drive doesn't work with personal google accounts, only shared drives. I can't create a shared drive with my account. I asked for WFFRC permission to share the relevant folders on the WFFRC drive. If they approve I'll need to change the folder IDs to reflect folders on WFFRC shared drive rather than Jazmin's personal drive where they are now.
# duplicate_check = drive.ListFile({'q':f"'{processed_folder_id}' in parents and title contains '{file_name}' and trashed=false"}).GetList()
# if not duplicate_check:
#   csv_buffer = StringIO()
#   data.to_csv(csv_buffer, index=False)
#   csv_buffer.seek(0)
#   gfile = drive.CreateFile({"title": f'{file_name}_test', "parents": [{"id": processed_folder_id}]})
#   gfile.SetContentString(csv_buffer.getvalue())
#   gfile.Upload()
#   print("CSV uploaded successfully!")

df = data.copy()

filter_options = ['Chamber A', 'Actual/SP']
group_options = ['chamber', 'actual_sp']
var_options = ['CO2', 'RH', 'Temp', 'PAR']

# filter = st.multiselect(label = 'Filter by:', options = filter_options, key = 'filter')
# group = st.multiselect(label = 'Group by:', options = group_options, key = 'group')
variables = st.multiselect(label = 'Select which variables to graph: ', options = var_options, key='variables', default = ['PAR', 'CO2'])
if len(variables) > 2:
  st.error("Can't select more than two variables at a time")

chamber = st.radio(label = 'Chamber', options = ['A', 'B', 'Both'], key = 'chamber_radio', index = 2)
actual_sp = st.radio(label = 'Actual or Setpoint', options = ['actual', 'sp', 'Both'], index = 2, key='actual_sp_radio')

if chamber != 'Both':
    df = df[df['Chamber'] == chamber]

if actual_sp != 'Both':
  df = df[df['actual_sp'] == actual_sp]

cols = ['minute', 'Chamber', 'actual_sp']+variables
df = df[cols]
st.write(df)
group = ['minute', 'Chamber', 'actual_sp']
df_grouped = df.groupby(group).agg({col:'mean' for col in variables})
df_grouped.reset_index(inplace=True)

fig = make_subplots(specs=[[{"secondary_y": True}]])
fig.add_trace(go.line(x=df['minute'], y = df[variables[0]], name = variables[0]), secondary_y=False)
fig.add_trace(go.line(x=df['minute'], y = df[variables[1]], name = variables[1]), secondary_y=True)
st.plotly_chart(fig)


# fig = px.line(df, x = 'minute', y = 
# fig_temp = px.line(df_temp_grouped, x = "DateTime", y = "Temperature", color = 'group', title = 'Soil Temperature Sensors')
# fig_temp.update_layout(xaxis_title = 'Time', yaxis_title = 'Temperature_(C)', height = 600)
# st.plotly_chart(fig_temp)





# # Update axis titles
# fig.update_yaxes(title_text="Left Y-axis", secondary_y=False)
# fig.update_yaxes(title_text="Right Y-axis", secondary_y=True)

# fig.show()





# for 
# data = data[data[filter]
# for term in filter:
#   data = data[data.isin([term]).any(axis=1)]

# for term in filter_temp:
#     df_temp = df_temp[df_temp.isin([term]).any(axis=1)]
