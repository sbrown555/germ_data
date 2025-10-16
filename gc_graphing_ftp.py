import pandas as pd
from pathlib import Path
import shutil
import re
import plotly.express as px 
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def google_drive_access_local():
  SERVICE_ACCOUNT_JSON = "/Users/sean/Documents/Sean/Lara Research/GC Data/operating-pod-469720-b9-214b1ebc73b3.json"
  with open(SERVICE_ACCOUNT_JSON) as f:
    sa_info = json.load(f)
  client_email = sa_info["client_email"]
  gauth = GoogleAuth()
  gauth.settings = {
    'client_config_backend': 'service',
    'service_config': {
        'client_json_file_path': SERVICE_ACCOUNT_JSON,
        'client_user_email': client_email
    },
    'oauth_scope': ['https://www.googleapis.com/auth/drive']
  }
  gauth.ServiceAuth()
  drive = GoogleDrive(gauth)
  return drive
  
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
  

def open_csvs(path):
  processed_files = set()
  data_list = []
  path = Path(path)
  for folder in path.iterdir():
    if folder.is_dir():
      chamber = folder.name.split('Chamber')[-1][0]
      # print('chamber=',chamber)
      for file in folder.iterdir():
        if file.suffix.lower() == '.log':
          if file.name not in processed_files:
            print(f'Reading file {file.name}')
            processed_files.add(file.name)
            data = pd.read_csv(file)
            data['chamber'] = chamber
            colnames = [str(col).strip().lower() for col in data.columns]
            # mask = data.apply(lambda row: [str(x).strip().lower() for x in row != colnames], axis=1)
            # data = data[mask]
            # data = data[~(data.astype(str) == list(data.columns)).all(axis=1)]
            data_list.append(data)
  if data_list:
    data_total = pd.concat(data_list, ignore_index=True)
    return data_total
  else:
    print('No CSVs found')
    return pd.DataFrame()

# # Looking through folder with processed data and finding that most recent 
# processed_folder_id = '11x8zo1ZQYU_MuFh2A36f4TmGYaojEnpZ'
# # processed_folder_id = '1v8Yu0IudRp_DiES99hVPMuF-Ma5lt5Zt'
# search_term = 'gc_data_processed'
# if processed_folder_id:
#     query = f"'{processed_folder_id}' in parents and title contains '{search_term}' and trashed=false"
# else:
#     query = f"title contains '{search_term}' and trashed=false"
# file_list = drive.ListFile({'q': query}).GetList()
# date_dict = {}
# for file in file_list:
#   match = re.search(r'\d{1,2}[A-Za-z]{3}\d{2}', file['title'])
#   if match:
#     date_str = match.group(0)
#     date = pd.to_datetime(date_str, format='%d%b%y')
#     date_dict[date] = file
#   else:
#     st.write(f"No valid date found in {file['title']}")
#   # date = file['title'].split('_')[-1].split('.')[0]
#   # date = pd.to_datetime(date, format = '%d%b%y')
#   # date_dict[date] = file
# last_processing_date = max(date_dict.keys())
# csv_id = date_dict[last_processing_date]['id']
# columns = ['minute', 'Chamber', 'actual_sp','Temp', 'RH', 'PAR', 'CO2']
# data_old = read_drive_id(csv_id, cols = columns)
# last_processing_time = data_old['minute'].max()

ethernet_folder_id = ''
search_term = ''
query = f"'{ethernet_folder_id}' in parents and title contains '{search_term}' and trashed=false"
folder_list = drive.ListFile({'q':query}).GetList()

{f['title']:f['id'] for f in folder_list}

st.selectbox('Select which folder to graph:', folder_list

for folder in folder_list:
  match = re.search(r'\d{8}', folder['title'])
  if match:
    date = match.group()
    
    data = 

  folder_id = folder['id']
  query = f"'{folder_id}' in parents and title contains '.log' and trashed=false"
  subfolder_list = drive.ListFile({'q':query}).GetList()
  for 
  
folder_name = d

match = re.search(r'\d{8}', 
search_term = '
df_list = []

ethernet_folder = '/Users/sean/Documents/Sean/Lara Research/GC Data/Downloaded Data/Ethernet_Data'

for folder in Path(ethernet_folder).iterdir():
  if folder.is_dir():
    match = re.search(r'\d{8}', folder.name)
    if match:
      date = match.group()
      data = open_csvs(folder)
      data['download_date']=date
      df_list.append(data)
data_ethernet = pd.concat(df_list)
data_ethernet.sort_values('download_date')
subset = [col for col in data_ethernet.columns if col != 'download_date']
data_ethernet.drop_duplicates(subset = subset, keep='first', inplace=True)



df = data_ethernet[['DATE', 'TIME', 'chamber', 'AI_TEMP', 'SP_TEMP', 'AI_HUM', 'SP_HUM', 'AI_LIGHT', 'SP_LIGHT1', 'AI_CO2', 'SP_CO2', 'download_date']].copy()
df.rename(columns = {'DATE':'date', 'TIME':'time', 'AI_TEMP':'temp_ac', 'SP_TEMP':'temp_sp', 'AI_HUM':'rh_ac', 'SP_HUM':'rh_sp', 'AI_LIGHT':'par_ac', 'SP_LIGHT1':'par_sp', 'AI_CO2':'co2_ac', 'SP_CO2':'co2_sp'}, inplace=True)
# data.columns
# df.columns

df['date'] = pd.to_datetime(df['date'], format = '%Y/%m/%d', errors='coerce').dt.date
df['time'] = pd.to_datetime(df['time'], format = '%H:%M:%S', errors='coerce').dt.time

datetimes = ['date', 'time']
variables = [col for col in df.columns if col not in datetimes and col != 'chamber' and col != 'download_date']
for col in variables:
  df[col] = pd.to_numeric(df[col], errors='coerce')

df.dropna(subset=datetimes+variables, inplace=True)

df['datetime'] = pd.to_datetime(df["date"].astype(str) + " " + df["time"].astype(str))
df_copy = df.copy()


df.drop(['date','time'], axis=1, inplace=True)

index = ['datetime','chamber', 'download_date']
variables = ['temp_ac', 'temp_sp','rh_ac','rh_sp','par_ac','par_sp', 'co2_ac','co2_sp']

df_long = df.melt(id_vars = index, value_vars = variables, var_name = 'variable_name', value_name = 'value')
df_long[['variable', 'actual_sp']] = df_long['variable_name'].str.split("_", expand=True)
df_long['actual_sp'].replace('ac', 'actual', inplace=True)
df_long.drop('variable_name', axis=1, inplace=True)
index = index+['actual_sp']
# index
# subset = [col for col in df_long.columns if col != 'downlaod_date']
# df_long.drop_duplicates(subset=subset, keep='first', inplace=True)

df_wide = df_long.pivot(index=index, columns = 'variable', values='value')
df_wide.reset_index(inplace=True)
# df_wide.columns

df_wide.sort_values('download_date')
df_wide.drop(columns=['download_date'], inplace=True)
subset = [col for col in df_wide.columns if col != 'downlaod_date']
df_wide.drop_duplicates(subset = subset, keep='first')
df_wide['temp'] = df_wide['temp']*10

# df = data_copy.copy()

df_wide.to_csv('/Users/sean/Documents/Sean/Lara Research/GC Data/Processed Data/gc_data_ethernet_processed_13Oct25.csv')

data_wide=df_wide.copy()e
