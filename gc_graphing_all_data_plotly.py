# Need to add actual xlim to graphs to keep axes from changing when data is missing
# Need to add ylim through a dictionary giving high and low limits for each variable. Default should be None

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
columns = ['minute', 'Chamber', 'actual_sp','Temp', 'RH', 'PAR', 'CO2']
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
    date_list.append(date)
    if date > last_processing_date:
      file_dict[date] = file
  except Exception as e:
    file_name = file['title']
    print(f"failed to review {file_name}: {e}")
current_date = max(date_list).strftime(format = '%d%b%y')

# Adding as-yet unprocessed data to the primary dataset
data = data_old
for date in sorted(file_dict.keys()):
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
  data = pd.concat([data_new, data])
  # With data downloaded individually, the above cause some duplicate rows possibly, although it doesn't really make sense to me why
  # data.drop_duplicates(subset=['minute', 'Chamber', 'actual_sp', 'CO2'])

data_to_download = data.copy()


# Check to see if there is already an up-to-date processed file, and if not save new processed file
file_name = f"gc_data_processed_{current_date}.csv"

duplicate_check = drive.ListFile({'q':f"'{processed_folder_id}' in parents and title contains '{file_name}' and trashed=false"}).GetList()
if not duplicate_check:
  st.write('There is an updated file!')
else:
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

# # Sliders and Toggles
# date_range = st.slider("Select date range", min_value=min_date, max_value=max_date, value=(min_date, max_date))
# date_low_limit = date_range[0]
# date_upper_limit = date_range[1]

# co2_range = st.slider('Select CO2 range: ', min_value=0, max_value=1500, value=(350, 800), key='co2_slider')
# rh_range = st.slider('Select RH range: ', min_value=0, max_value = 100, value=(1,100), key='rh_slider')
# par_range = st.slider('Select PAR range: ', min_value = 0, max_value = 1500, value=(0,1500), key = 'par_slider')
# temp_range = st.slider('Select Temperature range: ', min_value = 0, max_value = 500, value=(150,300), key='temp_slider')
# lim_dict = {'CO2':co2_range, 'RH':rh_range, 'PAR': par_range, 'Temp': temp_range}

def chamber_actual_check(chamber, actual):
  if chamber == 'A':
    co2_treatment = 'HiC'
  elif chamber == 'B':
    co2_treatment = 'LowC'
  else:
    st.error('Please select a chamber: A or B')
  if actual == True:
    actual_sp = 'Actual'
  elif actual == False:
    actual_sp = 'Set Point'
  else:
    st.error('Please specify if you want to graph actual or setpoint values')
  return [co2_treatment, actual_sp]

units = {'CO2':'ppm', 'Temp':'degrees C', 'RH':'%', 'PAR':'umol/mol'}
def plotly_graph(data1, data2, var1, var2, color1='blue', color2='red', title=None, x_range=None, units=units):
  fig = make_subplots(specs=[[{"secondary_y": True}]])
  fig.add_trace(go.Scatter(x=data1['minute'], y=data1[var1], name=var1, mode='lines', line=dict(color = color1)),secondary_y=False)
  fig.add_trace(go.Scatter(x=data2['minute'], y=data2[var2], name=var2, mode='lines', line=dict(color=color2)),secondary_y=True)
  fig.update_xaxes(title_text="Time", range=x_range)
  fig.update_yaxes(title_text=f'{var1} {(units[var1])}', secondary_y=False)
  fig.update_yaxes(title_text=f'{var2} ({units[var2]})', secondary_y=True)
  fig.update_layout(title=title)
  st.plotly_chart(fig, use_container_width=True)

def graph_plotly_var_par(df, chamber, actual, var1, var2='PAR', x_range=None, units = units):
  chamber_name = chamber_actual_check(chamber, actual)[0]
  df = df[(df['Chamber'] == chamber) & (df['actual_sp'] == ('actual' if actual else 'sp'))]
  title = f'{chamber_actual[1]} {var1} and {var2} in {chamber_name} Chamber'
  plotly_graph(df, df, var1, var2, x_range=x_range, units=units, title=title)

# Graphing variables with PAR
variables = ['CO2', 'RH', 'Temp']
for var in variables:
  for chamber in ['A','B']:
    graph_plotly_var_par(data, chamber, True, var, x_range = [min_date, max_date])

# SP vs actual (comparing setpoint and actual variables for each chamber)
def graph_actual_sp(df, var, chamber, color1='blue', color2='orange', x_range=None):
  chamber_name = chamber_actual_check(chamber, True)[0]
  df = df[df['Chamber'] == chamber]
  data1=df[df['actual_sp'] == 'actual']
  data2 = df[df['actual_sp'] == 'sp']
  title = f'Actual and Set Point of {var} in {chamber_name} Chamber'
  plotly_graph(data1, data2, var, var, color1=color1, color2=color2, title=title, x_range=x_range)

variables = ['CO2', 'RH', 'Temp', 'PAR']
for var in variables:
  for chamber in ['A', 'B']:
    graph_actual_sp(data, var, chamber)




variables = ['CO2', 'Temp', 'RH', 'PAR']
# var_low_bound = {'CO2':0, 'Temp':100, 'RH':25, 'PAR':0}
# var_upper_bound = {'CO2':1000, 'Temp':300, 'RH':90, 'PAR':1500}
# units = {'CO2':'ppm', 'Temp':'degrees C', 'RH':'%', 'PAR':'umol/mol'}
# date_low_limit = pd.to_datetime('2025-05-01')
# date_upper_limit = pd.to_datetime('2025-08-08')
date_format = mdates.DateFormatter('%m/%d')
for var in variables:
  fig, axes = plt.subplots(2,1)
  for chamber, group in data.groupby('Chamber'):
    axes[0].plot(group['minute'], group[var], label = chamber)
  axes[0].xaxis.set_major_formatter(date_format)
  axes[0].set_title(f'{var} Actual in Both Chambers')
  axes[0].legend(title = 'A=HiC, B=LowC')
  axes[0].set_ylim(lim_dict[var][0], lim_dict[var][1])
  axes[0].set_xlim(date_low_limit, date_upper_limit)
  for chamber, group in data_sp.groupby('Chamber'):
    axes[1].plot(group['minute'], group[var], label = chamber)
  axes[1].xaxis.set_major_formatter(date_format)
  axes[1].set_title(f'{var} Set Point in Both Chambers')
  axes[1].legend(title = 'A=HiC, B=LowC')
  axes[1].set_ylim(lim_dict[var][0], lim_dict[var][1])
  axes[1].set_xlim(date_low_limit, date_upper_limit)
  plt.subplots_adjust(hspace=0.5)  
  fig.tight_layout()
  # fig_name = f'/Users/sean/Documents/Sean/Lara Research/GC Data/GC Data Graphs/{var}_sp_vs_actual_{current_date}{additional_file_info}.png'
  # if save_figure == True:
    # plt.savefig(fig_name)
  st.pyplot(fig)
  plt.close(fig)

# SP and actual (comparing side by side the setpoint and the actual measurements)

variables = ['CO2', 'Temp', 'RH', 'PAR']
var_low_bound = {'CO2':0, 'Temp':100, 'RH':25, 'PAR':0}
var_upper_bound = {'CO2':1000, 'Temp':300, 'RH':90, 'PAR':1500}
units = {'CO2':'ppm', 'Temp':'degrees C', 'RH':'%', 'PAR':'umol/mol'}
# date_low_limit = pd.to_datetime('2025-05-01')
# date_upper_limit = pd.to_datetime('2025-08-08')
date_format = mdates.DateFormatter('%m/%d')
for var in variables:
  fig, axes = plt.subplots(2,1)
  for actual_sp, group in data_total[data_total['Chamber'] == 'A'].groupby('actual_sp'):
    axes[0].plot(group['minute'], group[var], label = actual_sp)
  axes[0].xaxis.set_major_formatter(date_format)
  axes[0].set_title(f'{var} Set Point and Actual in HiC Chamber (A)')
  axes[0].legend()
  axes[0].set_ylim(lim_dict[var][0], lim_dict[var][1])
  axes[0].set_xlim(date_low_limit, date_upper_limit)
  for actual_sp, group in data_total[data_total['Chamber'] == 'B'].groupby('actual_sp'):
    axes[1].plot(group['minute'], group[var], label = actual_sp)
  axes[1].xaxis.set_major_formatter(date_format)
  axes[1].set_title(f'{var} Set Point and Actual in LowC Chamber (B)')
  axes[1].legend()
  axes[1].set_ylim(lim_dict[var][0], lim_dict[var][1])
  axes[1].set_xlim(date_low_limit, date_upper_limit)
  plt.subplots_adjust(hspace=0.5)
  fig.tight_layout()
  # fig_name = f'/Users/sean/Documents/Sean/Lara Research/GC Data/GC Data Graphs/{var}_sp&actual_{current_date}.png'
  # if save_figure == True:
  #   plt.savefig(fig_name)
  st.pyplot(fig)
  plt.close(fig)



# # Creating dataframe of difference between chambers (A - B) and graphing the difference
# data.reset_index(inplace=True)
# data_sp.reset_index(inplace=True)
# data.set_index(['minute', 'Chamber'], inplace=True)
# data_sp.set_index(['minute', 'Chamber'], inplace=True)

# diff_sp = pd.DataFrame(index=data.index)

# variables = ['CO2', 'Temp', 'RH', 'PAR']

# for var in variables:
#   diff_sp[var] = data[var] - data_sp[var]
  
# diff_sp.dropna(how='any', inplace=True)
# diff_sp.reset_index(inplace=True)

# variables = ['CO2', 'Temp', 'RH', 'PAR']
# var_low_bound = {'CO2':0, 'Temp':0, 'RH':0, 'PAR':0}
# var_upper_bound = {'CO2':1000, 'Temp':300, 'RH':90, 'PAR':1500}
# units = {'CO2':'ppm', 'Temp':'degrees C', 'RH':'%', 'PAR':'umol/mol'}
# date_low_limit = pd.to_datetime('2025-05-01')
# date_upper_limit = pd.to_datetime('2025-08-08')
# date_format = mdates.DateFormatter('%m/%d')
# for var in variables:
#   fig, ax = plt.subplots(1,1)
#   for chamber, group in diff_sp.groupby('Chamber'):
#     ax.plot(group['minute'], group[var], label = chamber)
#   ax.xaxis.set_major_formatter(date_format)
#   ax.set_title(f'{var} Actual - Set Point in Both Chambers')
#   ax.legend(title = 'A=HiC, B=LowC')
#   ax.set_ylim(var_low_bound[var], var_upper_bound[var])
#   ax.set_xlim(date_low_limit, date_upper_limit)
#   # fig_name = f'/Users/sean/Documents/Sean/Lara Research/GC Data/GC Data Graphs/difference_between sp_and_actual_{var}_{current_date}{additional_file_info}.png'
#   # if save_figure == True:
#   #   plt.savefig(fig_name)
#   st.pyplot(fig)
#   plt.close(fig)


# # Creating a dataframe of the difference between actual and setpoint (a - sp) and graphing for each chamber.

# df_list = [data_a, data_b, data_sp_a, data_sp_b]

# for df in df_list:
#   for var in variables:
#     df[var] = pd.to_numeric(df[var])

# diff_actual = pd.DataFrame(index = data_a.index)
# diff_set = pd.DataFrame(index = data_sp_a.index)
# diff_actual = data_a.set_index('minute') - data_b.set_index('minute')
# diff_set = data_sp_a.set_index('minute') - data_sp_b.set_index('minute')
# diff_actual['actual_sp'] = 'actual'
# diff_set['actual_sp'] = 'sp'

# diff_total = pd.concat([diff_actual, diff_set])
# diff_total.reset_index(inplace=True)

# # plt.clf()
# # fig, axes = plt.subplots(2,2)
# # axes = axes.flatten()
# # for i, var in enumerate(variables):
# #   for actual_sp, group in diff_total.groupby('actual_sp'):
# #     axes[i].plot(group['minute'], group[var], label = actual_sp)
# #   axes[i].legend()
# # plt.show()

# for var in variables:
#   fig, axes = plt.subplots(1,1)
#   for actual_sp, group in diff_total.groupby('actual_sp'):
#     axes.plot(group['minute'], group[var], label = actual_sp)
#   axes.legend()
#   axes.set_title(f'Difference Between Chamber A and Chamber B in {var}')
#   date_format = mdates.DateFormatter('%m/%d')
#   axes.xaxis.set_major_formatter(date_format)
#   st.pyplot(fig)
#   # fig_name=f'/Users/sean/Documents/Sean/Lara Research/GC Data/GC Data Graphs/difference_between_chambers_{var}_{current_date}{additional_file_info}.png'
#   # if save_figure == True:
#   #   plt.savefig(fig_name)
#   plt.close(fig)
