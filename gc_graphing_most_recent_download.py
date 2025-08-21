from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import json
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
# import requests
from io import StringIO
import streamlit as st
import os

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

# Looking through "Chamber Data folder and finding most recent download date
folder_id = "11Cdt-JEEeNaDLNdFj002mWmt5BgnHBrO"
file_list = drive.ListFile({'q': f"'{folder_id}' in parents and trashed=false"}).GetList()
date_list = []
file_dict = {}
for file in file_list:
  try: 
    date = pd.to_datetime(file['title'].split('_')[0])
    date_list.append(date)
    file_dict[date] = file
  except Exception as e:
    file_name = file['title']
    print(f"failed to read {file_name}: {e}")
    
newest_folder = file_dict[max(file_dict.keys())]
newest_folder_id = newest_folder['id']


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

def data_from_date(date_folder, time_offset, actual):
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
      print(f"chamber folder: {chamber_folder['title']}")
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
                try:
                  try:
                    df = pd.read_csv(StringIO(csv_content))
                  except UnicodeDecodeError:
                    df = pd.read_csv(csv_file, encoding='latin1', engine = 'python', on_bad_lines='skip')
                    # problem = df
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




time_offset = [pd.Timedelta(days=0), pd.Timedelta(days=0)]

df = data_from_date(newest_folder, time_offset, actual=True)


# st.write(df.head())

Graphing

# Regular graphing of variables with PAR for each chamber
# Graphing zoomed CO2/PAR

plt.clf()
fig, axes = plt.subplots(2,1, figsize=(8,6))

ax1 = axes[0]
ax1.plot(data_a['minute'], data_a['CO2'], 'b-', label='CO2')
ax1.set_ylabel('CO2', color = 'b')
ax1.tick_params(axis='y', labelcolor='b')
ax1.set_ylim(680, 800)

ax2=ax1.twinx()
ax2.plot(data_a['minute'], data_a['PAR'], 'r-', label='PAR')
ax2.set_ylabel('PAR', color = 'r')
ax2.tick_params(axis='y', labelcolor='r', rotation = 45)
ax2.set_ylim(0,1500)

ax3 = axes[1]
ax3.plot(data_b['minute'], data_b['CO2'], 'b-', label = 'CO2')
ax3.set_ylabel('CO2', color = 'b')
ax3.set_xlabel('date')
ax3.tick_params(axis='y', labelcolor = 'b')
ax3.set_ylim(400, 500)

ax4 = ax3.twinx()
ax4.plot(data_b['minute'], data_b['PAR'], 'r-', label = 'PAR')
ax4.set_ylabel('PAR', color = 'r')
ax4.tick_params(axis='y', labelcolor = 'r', rotation = 45)
ax4.set_ylim(0,1500)

date_format = mdates.DateFormatter('%m/%d')

for ax in axes:
  ax.xaxis.set_major_formatter(date_format)
  
axes[0].set_title('Elevated CO2 Chamber (HiC): 650-750 ppm')
axes[1].set_title('Ambient CO2 Chamber (LowC): 400-500 ppm')
fig.suptitle('CO2 and PAR in Both Chambers')
  
# axes[0].set_ylim(650, 750)
# axes[1].set_ylim(400, 500)

fig.tight_layout()
st.pyplot(fig)

fig_name = f'/Users/sean/Documents/Sean/Lara Research/GC Data/GC Data Graphs/co2_par_zoomed_in_{current_date}{additional_file_info}.png'
# if save_figure == True:
  # plt.savefig(fig_name)

# Graphing all variables with PAR. Not zoomed in

variables = ['CO2','RH','Temp']
for var in variables:
  
  plt.clf()
  fig, axes = plt.subplots(2,1, figsize=(8,6))
  # var_low_bound_lowc = 350
  # var_up_bound_lowc = 900
  # var_low_bound_hic = 350
  # var_up_bound_hic = 900
  # ax1.set_ylim(var_low_bound_hic, var_up_bound_hic)
  # ax3.set_ylim(var_low_bound_lowc, var_up_bound_lowc)
  ax1 = axes[0]
  ax1.plot(data_a['minute'], data_a[var], 'b-', label=var)
  ax1.set_ylabel(var, color = 'b')
  ax1.tick_params(axis='y', labelcolor='b')
  ax2=ax1.twinx()
  ax2.plot(data_a['minute'], data_a['PAR'], 'r-', label='PAR')
  ax2.set_ylabel('PAR', color = 'r')
  ax2.tick_params(axis='y', labelcolor='r', rotation = 45)
  ax2.set_ylim(0,1500)
  ax3 = axes[1]
  ax3.plot(data_b['minute'], data_b[var], 'b-', label = var)
  ax3.set_ylabel(var, color = 'b')
  ax3.set_xlabel('date')
  ax3.tick_params(axis='y', labelcolor = 'b')
  ax4 = ax3.twinx()
  ax4.plot(data_b['minute'], data_b['PAR'], 'r-', label = 'PAR')
  ax4.set_ylabel('PAR', color = 'r')
  ax4.tick_params(axis='y', labelcolor = 'r', rotation = 45)
  ax4.set_ylim(0,1500)
  date_format = mdates.DateFormatter('%m/%d')
  for ax in axes:
    ax.xaxis.set_major_formatter(date_format)
    axes[0].set_title(f'{var} Elevated CO2 Chamber (HiC)')
    axes[1].set_title(f'{var} Ambient CO2 Chamber (LowC)')
    fig.suptitle(f'{var} and PAR in Both Chambers')
    # axes[0].set_ylim(650, 750)
    # axes[1].set_ylim(400, 500)
  fig.tight_layout()
  st.pyplot(fig)
  fig_name = f'/Users/sean/Documents/Sean/Lara Research/GC Data/GC Data Graphs/{var}_par_{current_date}{additional_file_info}.png'
  # if save_figure == True:
    # plt.savefig(fig_name)
  

# SP and actual conditionals comparison

# SP vs actual (comparing setpoint and actual variables for each chamber)
variables = ['CO2', 'Temp', 'RH', 'PAR']
var_low_bound = {'CO2':0, 'Temp':100, 'RH':25, 'PAR':0}
var_upper_bound = {'CO2':1000, 'Temp':300, 'RH':90, 'PAR':1500}
units = {'CO2':'ppm', 'Temp':'degrees C', 'RH':'%', 'PAR':'umol/mol'}
# date_low_limit = pd.to_datetime('2025-05-01')
# date_upper_limit = pd.to_datetime('2025-08-08')
date_format = mdates.DateFormatter('%m/%d')
for var in variables:
  plt.clf()
  fig, axes = plt.subplots(2,1)
  for chamber, group in data.groupby('Chamber'):
    axes[0].plot(group['minute'], group[var], label = chamber)
  axes[0].xaxis.set_major_formatter(date_format)
  axes[0].set_title(f'{var} Actual in Both Chambers')
  axes[0].legend(title = 'A=HiC, B=LowC')
  axes[0].set_ylim(var_low_bound[var], var_upper_bound[var])
  # axes[0].set_xlim(date_low_limit, date_upper_limit)
  for chamber, group in data_sp.groupby('Chamber'):
    axes[1].plot(group['minute'], group[var], label = chamber)
  axes[1].xaxis.set_major_formatter(date_format)
  axes[1].set_title(f'{var} Set Point in Both Chambers')
  axes[1].legend(title = 'A=HiC, B=LowC')
  axes[1].set_ylim(var_low_bound[var], var_upper_bound[var])
  # axes[1].set_xlim(date_low_limit, date_upper_limit)
  plt.subplots_adjust(hspace=0.5)  
  fig_name = f'/Users/sean/Documents/Sean/Lara Research/GC Data/GC Data Graphs/{var}_sp_vs_actual_{current_date}{additional_file_info}.png'
  # if save_figure == True:
    # plt.savefig(fig_name)
  st.pyplot(fig)

# SP and actual (comparing side by side the setpoint and the actual measurements)

data['actual_sp'] = 'actual'
data_sp['actual_sp'] = 'sp'
data_total = pd.concat([data, data_sp])
variables = ['CO2', 'Temp', 'RH', 'PAR']
var_low_bound = {'CO2':0, 'Temp':100, 'RH':25, 'PAR':0}
var_upper_bound = {'CO2':1000, 'Temp':300, 'RH':90, 'PAR':1500}
units = {'CO2':'ppm', 'Temp':'degrees C', 'RH':'%', 'PAR':'umol/mol'}
date_low_limit = pd.to_datetime('2025-05-01')
date_upper_limit = pd.to_datetime('2025-08-08')
date_format = mdates.DateFormatter('%m/%d')
for var in variables:
  plt.clf()
  fig, axes = plt.subplots(2,1)
  for actual_sp, group in data_total[data_total['Chamber'] == 'A'].groupby('actual_sp'):
    axes[0].plot(group['minute'], group[var], label = actual_sp)
  axes[0].xaxis.set_major_formatter(date_format)
  axes[0].set_title(f'{var} Set Point and Actual in HiC Chamber (A)')
  axes[0].legend()
  axes[0].set_ylim(var_low_bound[var], var_upper_bound[var])
  # axes[0].set_xlim(date_low_limit, date_upper_limit)
  for actual_sp, group in data_total[data_total['Chamber'] == 'B'].groupby('actual_sp'):
    axes[1].plot(group['minute'], group[var], label = actual_sp)
  axes[1].xaxis.set_major_formatter(date_format)
  axes[1].set_title(f'{var} Set Point and Actual in LowC Chamber (B)')
  axes[1].legend()
  axes[1].set_ylim(var_low_bound[var], var_upper_bound[var])
  # axes[1].set_xlim(date_low_limit, date_upper_limit)
  plt.subplots_adjust(hspace=0.5)  
  fig_name = f'/Users/sean/Documents/Sean/Lara Research/GC Data/GC Data Graphs/{var}_sp&actual_{current_date}.png'
  # if save_figure == True:
  #   plt.savefig(fig_name)
  st.pyplot(fig)


# Creating dataframe of difference between chambers (A - B) and graphing the difference
data.reset_index(inplace=True)
data_sp.reset_index(inplace=True)
data.set_index(['minute', 'Chamber'], inplace=True)
data_sp.set_index(['minute', 'Chamber'], inplace=True)

diff_sp = pd.DataFrame(index=data.index)

variables = ['CO2', 'Temp', 'RH', 'PAR']

for var in variables:
  diff_sp[var] = data[var] - data_sp[var]
  
diff_sp.dropna(how='any', inplace=True)
diff_sp.reset_index(inplace=True)

variables = ['CO2', 'Temp', 'RH', 'PAR']
var_low_bound = {'CO2':0, 'Temp':0, 'RH':0, 'PAR':0}
var_upper_bound = {'CO2':1000, 'Temp':300, 'RH':90, 'PAR':1500}
units = {'CO2':'ppm', 'Temp':'degrees C', 'RH':'%', 'PAR':'umol/mol'}
date_low_limit = pd.to_datetime('2025-05-01')
date_upper_limit = pd.to_datetime('2025-08-08')
date_format = mdates.DateFormatter('%m/%d')
for var in variables:
  plt.clf()
  fig, ax = plt.subplots(1,1)
  for chamber, group in diff_sp.groupby('Chamber'):
    ax.plot(group['minute'], group[var], label = chamber)
  ax.xaxis.set_major_formatter(date_format)
  ax.set_title(f'{var} Actual - Set Point in Both Chambers')
  ax.legend(title = 'A=HiC, B=LowC')
  ax.set_ylim(var_low_bound[var], var_upper_bound[var])
  # ax.set_xlim(date_low_limit, date_upper_limit)
  fig_name = f'/Users/sean/Documents/Sean/Lara Research/GC Data/GC Data Graphs/difference_between sp_and_actual_{var}_{current_date}{additional_file_info}.png'
  # if save_figure == True:
  #   plt.savefig(fig_name)
  st.pyplot(fig)


# Creating a dataframe of the difference between actual and setpoint (a - sp) and graphing for each chamber.

df_list = [data_a, data_b, data_sp_a, data_sp_b]

variables = ['CO2', 'Temp', 'RH', 'PAR']
cols = ['minute'] + variables
data_a = data_a[cols]
data_b = data_b[cols]
data_sp_a = data_sp_a[cols]
data_sp_b = data_sp_b[cols]

for df in df_list:
  for var in variables:
    df[var] = pd.to_numeric(df[var])

diff_actual = pd.DataFrame(index = data_a.index)
diff_set = pd.DataFrame(index = data_sp_a.index)
diff_actual = data_a.set_index('minute') - data_b.set_index('minute')
diff_set = data_sp_a.set_index('minute') - data_sp_b.set_index('minute')
diff_actual['actual_sp'] = 'actual'
diff_set['actual_sp'] = 'sp'

diff_total = pd.concat([diff_actual, diff_set])
diff_total.reset_index(inplace=True)

# plt.clf()
# fig, axes = plt.subplots(2,2)
# axes = axes.flatten()
# for i, var in enumerate(variables):
#   for actual_sp, group in diff_total.groupby('actual_sp'):
#     axes[i].plot(group['minute'], group[var], label = actual_sp)
#   axes[i].legend()
# plt.show()

for var in variables:
  plt.clf()
  fig, axes = plt.subplots(1,1)
  for actual_sp, group in diff_total.groupby('actual_sp'):
    axes.plot(group['minute'], group[var], label = actual_sp)
  axes.legend()
  axes.set_title(f'Difference Between Chamber A and Chamber B in {var}')
  date_format = mdates.DateFormatter('%m/%d')
  axes.xaxis.set_major_formatter(date_format)
  st.pyplot(fig)
  fig_name=f'/Users/sean/Documents/Sean/Lara Research/GC Data/GC Data Graphs/difference_between_chambers_{var}_{current_date}{additional_file_info}.png'
  # if save_figure == True:
  #   plt.savefig(fig_name)
  
