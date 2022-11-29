import os
import sys
import numpy as np
import pandas as pd
import datetime
from edwards.loader import Odbc
import pyodbc
import re
import types
from termcolor import colored
import types
import re
import os
from bokeh import plotting
from bokeh import layouts
from bokeh import io
from bokeh import models
from bokeh.models.tools import HoverTool
from bokeh.models.widgets.markups import Div
from math import ceil
import math
from sympy.ntheory import primefactors

# Prompts, Inputs and Data Retrieval #

## Login ##

login_file_path = os.path.join(sys.path[0][:sys.path[0].rindex('\\')], r'Resources\login.json')

db = Odbc.load_login(file=login_file_path)

## Obtain list of all databases with relevant information ##

master_db = 'master'

master_db_connection = pyodbc.connect('Driver={SQL Server};'
                                'Server=' + db.server + ';'
                                 'Database=' + master_db + ';'
                                 'Uid=' + db.uid + ';'
                                 'Pwd=' + db.pwd + ';')

get_databases_query = ("select name \
                        from master.sys.databases \
                        where name like '%scada_production%'")

customer_data_databases = pd.read_sql_query(get_databases_query, master_db_connection)

## Prompt user for the customer/database of interest ##

confirmation = 'N'
while confirmation == 'N':
    print(customer_data_databases.iloc[1:])
    choices = customer_data_databases.iloc[1:].index.to_list()
    user_choice = int(input('\nUsing the the table displayed, please enter the number corresponding to the customer database of interest:\n'))
    while user_choice not in choices:
        print('\nInvalid choice. Please choose a number associated with one of the databases listed below: \n')
        print(customer_data_databases.iloc[1:])
        user_choice = input()
    confirmation = input(f'You have chosen "{customer_data_databases.iloc[user_choice][0]}". Is this correct (Y/N)? ').upper()
database = customer_data_databases.iloc[user_choice][0]

MAIN_FOLDER_PATH = fr"{input('Please enter the path of the directory within which you wish the data and figures to be saved: ')}".replace('"', '').replace("'",'')
print(MAIN_FOLDER_PATH)
while not os.path.exists(MAIN_FOLDER_PATH):
    print('\n')
    MAIN_FOLDER_PATH = fr"{input('The entered path does not exist. Please enter a valid path to continue: ')}"

## Get inputs function ##

def get_input(prompt, success_conditions=None, failure_messages=None, message=None, wrapping_func=None):
    
    
    if wrapping_func == None:
        wrapping_func = lambda x: x
    
    if message is not None:
        print(message)
    
    if success_conditions == None:
        return wrapping_func(input(prompt))
    
    else:
        x = input(prompt)
        
        num_conditions_to_satisfy = len(success_conditions)

        success_count = 0

        while success_count < num_conditions_to_satisfy:
        
            for idx in range(num_conditions_to_satisfy):
                # print(f'Success condition {idx} is {success_conditions[idx](x)} because x is {x} and its type is {type(x)}')
                if isinstance(success_conditions[idx], types.FunctionType):
                    if success_conditions[idx](x):
                        success_count+=1
                    else:
                        if len(failure_messages) > 1:
                            print(failure_messages[idx].format(x, type(x), f'Condition {idx+1} failure'))
                            return get_input(prompt, 
                                            success_conditions=success_conditions, 
                                            failure_messages=failure_messages, 
                                            message=message,
                                            wrapping_func=wrapping_func)
                        else:
                            print(failure_messages[0])
                            return get_input(prompt, 
                                             success_conditions=success_conditions, 
                                             failure_messages=failure_messages, 
                                             message=message,
                                            wrapping_func=wrapping_func)
                
                else:
                    if success_conditions[idx]:
                        success_count+=1
                    else:
                        if len(failure_messages) > 1:
                            print(failure_messages[idx].format(x, type(x), f'Condition {idx+1} failure'))
                            return get_input(prompt, 
                                            success_conditions=success_conditions, 
                                            failure_messages=failure_messages, 
                                            message=message,
                                            wrapping_func=wrapping_func)
                        else:
                            print(failure_messages[0])
                            return get_input(prompt, 
                                             success_conditions=success_conditions, 
                                             failure_messages=failure_messages, 
                                             message=message,
                                            wrapping_func=wrapping_func)
        print(f'Thank you for choosing {wrapping_func(x)}')
        return wrapping_func(x)

## Establish connection with the chosen customer data connection ##

db_connection = pyodbc.connect('Driver={SQL Server};'
                                'Server=' + db.server + ';'
                                 'Database=' + database + ';'
                                 'Uid=' + db.uid + ';'
                                 'Pwd=' + db.pwd + ';')

## Define a function that checks whether a string can be converted to an integetr or not ##

def isintable(x):
    try:
        x = int(x)
        return True
    except:
        False

## Prompt user for whether they want to obtain system data based on system types, system names or both ##

type_or_name_or_both = get_input(prompt=f'Please choose whether you wish to proceed by \n1. System Name; \n2. System Type; \n3. Both.\nAlternatively, if you wish to retrieve data for all systems within {database}, please enter 4.\n',
                                 success_conditions=(isintable, lambda x: int(x) in (1,2,3,4)),
                                 failure_messages=('{2}: {0} is not a valid choice. Please choose from 1, 2, 3 and 4 because {0} cannot be converted into an integer.',
                                                   '{2}: {0} is not a valid choice. Please choose from 1, 2, 3 and 4 because {0} is not in (1,2,3,4).'),
                                 message=None,
                                 wrapping_func=int)

if type_or_name_or_both in (1,3):
    systems_by_file = get_input(prompt='Would you like to \n0. Enter the system names here (enter 0); or \n1. Pass a file path containing the system names (enter 1)\n',
                                success_conditions=(isintable, lambda x: int(x) in (0,1)),
                                failure_messages=('{2}: {0} is not a valid choice as it cannot be converted into an integer. Please choose from 0 or 1.',
                                                  '{2}: {0} is noT a valid choice as it is neither 0 nor 1. Please choose 0 or 1.'))
    systems_by_file = bool(systems_by_file)

get_system_names_bool_args = {'by_names':bool(type_or_name_or_both==1),
                              'by_types':bool(type_or_name_or_both==2),
                              'systems_by_file':bool(type_or_name_or_both==3),
                              'everything':bool(type_or_name_or_both==4)}


## Define a function for retrieving system names form a comma separated text file ##

def systems_from_csv(PATH):
    systems = []

    SYSTEMS_FILE_PATH = PATH

    with open(SYSTEMS_FILE_PATH, 'r') as systems_file:
        for line in systems_file:
            line_systems = re.split(',|, | ,| , ', line)
            systems = systems.copy() + line_systems.copy()

    for i in range(len(systems)):
        if '\'' in systems[i] or '\"' in systems[i]:
            systems[i] = systems[i].replace('\'', '')
            systems[i] = systems[i].replace('\"', '')

## Prompt user for systems/system types/both and ensure at least one corresponding system exists ##

def get_system_names(db_connection, database, by_names=False, by_types=False, systems_by_file=False, everything=False):
    if by_names:
        if not systems_by_file:
            systems = get_input(prompt='Please enter the names of the systems for which you wish to retrieve data, separated by commas.\n')
            systems = re.split(',| ,|, | , ', systems)
            systems_as_string = str(systems).replace('[','(').replace(']',')')


            if by_types:
                system_types = get_input(prompt='Please enter the types of the systems for which you wish to retrieve data, separated by commas.\n')
                system_types = re.split(',| ,| , ', system_types)
                system_types_as_string = str(system_types).replace('[', '(').replace(']', ')')
                
                check_system_name_query = f'select * from {database}.dbo.fst_GEN_system \
                                            where Description in {systems_as_string} \
                                            and SystemTypeID in {system_types_as_string} \
                                            order by SystemTypeID'
            
            else:
                check_system_name_query = f'select * from {database}.dbo.fst_GEN_system \
                                            where Description in {systems_as_string} \
                                            order by SystemTypeID'

        else:
            systems_path = get_input(prompt='Please enter the file location for a comma separated file containing the names of the systems of interest:\n',
                                    success_conditions=(os.path.exists,))
            systems = systems_from_csv(systems_path)
            systems_as_string = str(systems).replace('[','(').replace(']',')')

            if by_types:
                system_types = get_input(prompt='Please enter the types of the systems for which you wish to retrieve data, separated by commas.\n')
                system_types = re.split(',| ,| , ', system_types)
                system_types_as_string = str(system_types).replace('[','(').replace(']',')')
                check_system_name_query = f'select * from {database}.dbo.fst_GEN_system \
                                                where Description in {systems_as_string} \
                                                and SystemTypeID in {system_types_as_string} \
                                                order by SystemTypeID'
            else:
                check_system_name_query = f'select * from {database}.dbo.fst_GEN_system \
                                            where Description in {systems_as_string} \
                                            order by SystemTypeID'

        query_result = pd.read_sql_query(check_system_name_query, db_connection)

        valid_systems = list(query_result['Description'].unique())

        invalid_systems = list(set(systems) - set(valid_systems))

        if len(invalid_systems) > 1:
            print('checking invalid systems')
            if len(valid_systems) < 1:
                print(f'Could not find any systems within {database} corresponding to the system names and types (if any entered) provided. Please try again.')
                return get_system_names(db_connection=db_connection, 
                                        database=database, 
                                        by_names=by_names, 
                                        by_types=by_types, 
                                        systems_by_file=systems_by_file,
                                        everything=everything)
            else:
                print(f'Could not find any systems within {database} corresponding to the entered system types (if any were entered) and following system names: {invalid_systems}.')
                print(f'Valid system names: {valid_systems}.\nWill retrieve data for these systems.')
                return query_result, valid_systems
        else:
            print(f'Found systems corresponding to all of the entered names within the {database} database; namely: \n{valid_systems}\nWill retrieve data for these systems.')
            return query_result, valid_systems

    elif by_types:
        system_types = get_input(prompt='Please enter the types of the systems for which you wish to retrieve data, separated by commas.\n')
        system_types = re.split(',| ,| , ', system_types)
        system_types_as_string = str(system_types).replace('[','(').replace(']',')')
        check_system_name_query = f'select * from {database}.dbo.fst_GEN_system \
                                    where SystemTypeID in {system_types_as_string} \
                                    order by SystemTypeID'

        query_result = pd.read_sql_query(check_system_name_query, db_connection)

        if len(query_result) > 0:
            type_systems = list(query_result[['Description', 'SystemTypeID']])
            print(f'The following systems were found to correspond with system types {system_types}:')
            print(type_systems)
            print('Will retrieve data for these systems.')
            return query_result, list(query_result.Description.unique())
        
        else:
            print(f'Could not find any systems within {database} whose system type IDs correspond with any of {system_types}. Please try again.')
            return get_system_names(db_connection=db_connection, 
                                    database=database, 
                                    by_names=by_names, 
                                    by_types=by_types, 
                                    systems_by_file=systems_by_file,
                                    everything=everything)
    elif everything:
        check_system_name_query = f'select * from {database}.dbo.fst_GEN_System \
                                    order by SystemTypeID'
        query_result = pd.read_sql_query(check_system_name_query, db_connection)
        print(f'{len(query_result)} systems were found within {database}, including:')
        print(query_result[['Description','SystemTypeID']])
        print('Will retrieve data for these systems.')
        return query_result, list(query_result.Description.unique())

systems_info, systems = get_system_names(db_connection=db_connection,
                                         database=database,
                                         **get_system_names_bool_args)

## Obtain parameter information ##

def get_parameters(systems, database, db_connection):

    systems_parameter_info = {}
        
    for idx in range(len(systems)):

        sys = systems[idx]

        get_parameters_query = (f"select DISTINCT a.SystemID, a.SystemTypeID, a.Description [SystemName], a.LastAlertLogTime, c.ParameterNumber, c.zzDescription,  c.SIUnitID\
                                from {database}..fst_GEN_System a \
                                join [{database}].[dbo].[fst_GEN_Parameter] b \
                                    on a.SystemTypeID = b.SystemTypeID \
                                join [{database}].[dbo].[fst_GEN_ParameterType] c \
                                    on b.SystemTypeID = c.SystemTypeID \
                                    and b.ParameterNumber = c.ParameterNumber \
                                where a.Description = \'{sys}\' \
                                order by a.SystemTypeID, a.Description")

        parameter_information = pd.read_sql_query(get_parameters_query, db_connection)
        
        systems_parameter_info[sys] = parameter_information
        
    return systems_parameter_info

## Create a parameter mapping dictionary for all systems ##

systems_parameter_info = get_parameters(systems=systems, database=database, db_connection=db_connection)

systems_parameter_info['param_mapping'] = {}
for system in systems:
    zipped = list(zip(systems_parameter_info[system]['ParameterNumber'], systems_parameter_info[system]['zzDescription']))
    systems_parameter_info['param_mapping'][system] = dict(zipped)

systems_parameter_info['param_mapping']
systems_to_check = list(systems_parameter_info['param_mapping'].keys())
for key in systems_to_check:
    if len(systems_parameter_info['param_mapping'][key]) == 0:
        print(f'Removing \'{key}\' from the list of systems for which data will be retrieved because it has no parameter informaiton.')
        systems_parameter_info['param_mapping'].pop(key)

### Partition Parameters by the Associated Mechanical Part ###

systems = list(systems_parameter_info['param_mapping'].keys())
params_dict_partitioned = {system:{} for system in systems}
for system in systems:
    # print(systems_parameter_info['param_mapping'].keys())
    # print(system)
    # print(system in list(systems_parameter_info['param_mapping'].keys()))
    params_sys = systems_parameter_info['param_mapping'][system]
    params_sys_dict = {'DryPump':{},
                       'Booster':{},
                       'ExhaustAndShaft':{},
                       'Flow':{},
                       'RunTime':{},
                       'Oil':{},
                       'Others':{}}
    for sys in params_sys.keys():
        params_sys[sys] = params_sys[sys].replace(' ', '').replace('DP', 'DryPump').replace('MB', 'Booster')
        if 'DP' in params_sys[sys] or 'Dry' in params_sys[sys]:
            params_sys_dict['DryPump'][sys]=params_sys[sys]

        elif 'Booster' in params_sys[sys] or 'MB' in params_sys[sys]:
            params_sys_dict['Booster'][sys]=params_sys[sys]

        elif 'Exhaust' in params_sys[sys] or 'Shaft' in params_sys[sys]:
            params_sys_dict['ExhaustAndShaft'][sys]=params_sys[sys]

        elif 'Time' in params_sys[sys] or 'Hours' in params_sys[sys]:
            params_sys_dict['RunTime'][sys]=params_sys[sys]

        elif 'Oil' in params_sys[sys]:
            params_sys_dict['Oil'][sys]=params_sys[sys]
            
        elif 'Flow' in params_sys[sys]:
            params_sys_dict['Flow'][sys]=params_sys[sys]

        else:
            params_sys_dict['Others'][sys]=params_sys[sys]
    # for key in params_sys_dict.keys():
    #     params_sys_dict[key].sort()
    params_dict_partitioned[system] = params_sys_dict

## Define directory paths ##

AVAIL_FOLDER_PATH = os.path.join(MAIN_FOLDER_PATH, f'Availability')
DATA_FOLDER_PATH = os.path.join(MAIN_FOLDER_PATH, 'Data')

## Retrieve data for each system to store in parquet files ##

systems_with_data = []
systems_withou_data = []
systems_param_mapping = {}
for system in systems:

    if not os.path.exists(DATA_FOLDER_PATH):
        os.mkdir(DATA_FOLDER_PATH)
    assert os.path.exists(DATA_FOLDER_PATH)
    print(f'Retrieving data for {system}.')
    system_parameters = list(systems_parameter_info['param_mapping'][f'{system}'].keys())
    print(f'Parameters: \n{system_parameters}')
    system_data = db.get_data(database=database,
                              system_name=system,
                              parameter_number=system_parameters)
    system_param_mapping = db.get_parameter_info(database=database,
                                                 system_name=system)
    
    systems_param_mapping[system] = dict(zip(system_param_mapping['ParameterNumber'].to_list(), system_param_mapping['zzDescription'].to_list()))

    if system_data is not None:
        file_path = os.path.join(DATA_FOLDER_PATH, system+'.parquet')
        system_data.to_parquet(file_path, compression=None)
        print(f'Data retrieved for {system}.')
        systems_with_data.append(system)
    
    else:
        print(f'There is no available data for {system}.')
        systems_withou_data.append(system)
        
for sys in systems_param_mapping.keys():
    sys_param_nums = list(systems_param_mapping[sys].keys())
    for part in params_dict_partitioned[sys].keys():
        for param_num in params_dict_partitioned[sys][part].keys():
            if param_num in sys_param_nums:
                params_dict_partitioned[sys][part][param_num] = systems_param_mapping[sys][param_num].replace(' ', '')
            else:
                params_dict_partitioned[sys][part].pop([param_num])

systems_in_dict = list(params_dict_partitioned.keys())
for sys in systems_in_dict:
    if sys not in systems_with_data:
        params_dict_partitioned.pop(sys)

# Visualisation #

DATA_FILES_DIR = DATA_FOLDER_PATH
FIG_DIR = os.path.join(MAIN_FOLDER_PATH,'Figures')
CSV_DIR = AVAIL_FOLDER_PATH
if not os.path.exists(FIG_DIR):
    os.mkdir(FIG_DIR)
if not os.path.exists(CSV_DIR):
    os.mkdir(CSV_DIR)

## Prepare the data in the parquet files for plotting ##

def plot_prep_from_parquet(data_files_dir, include_system_names_like=None):

    """
    Prepares the data that has already been written to parquet files for plotting. In particular, 
    this function separates the data for each system by swap date. It does so by partitioning the
    provisioned data by any columns whose name contains any of the following substrings:
    ('Run Hours', 'Time')
    
    Inputs:
    - data_files_dir (str): Full path or path from current working directory where the parquet files containing the system parametric data are stored.
    
    - include_system_names_like (str or list(str)): String or list of strings of the types of system names whose data we wish to  prepare. For instance, to prepare data for the systems that contain the substring 'iH1000' and 'iH2000', pass ['iH1000', 'iH2000']. DEFAULT is None and, in this case, all of the files in the provisioned directory are parsed.
    
    Outputs:
    - all_systems_data: A dictionary containing the data for the specified types of system names (if any were passed) partitioned by swap dates (if any swaps occurred).
    
    NB: If a system does not have any columns with the substring 'Run Hours' or 'Time', the system will be assumed to not be a pump and therefore skipped. Its data will not appear in the output.
    
    """

    files = os.listdir(data_files_dir)
    if include_system_names_like != None:
        if type(include_system_names_like)==str:
            files = [file for file in files if include_system_names_like in file]

        elif type(include_system_names_like)==list or type(include_system_names_like)==tuple:
            files = [file for file in files if any(name_like in file for name_like in include_system_names_like)]

        else:
            raise TypeError(f"The argument `include_system_names_like` does not take assignments of type {type(include_system_names_like)}. Please pass a string or list of strings to this argument.")

    system_names=[]
    all_systems_data = {}

    for file_name in files:

        # Get the system data and format correctly:

        system_name = re.split(r'\.', file_name)[0]
        system_names.append(system_name)
        system_data = pd.read_parquet(os.path.join(data_files_dir, file_name))
        print(f'Preparing the {system_name} data for plotting.')
        
        # Convert DataFrame from long to wide format and sort by LogTime:
        
        system_data = system_data.pivot_table(index='LogTime',
                                              columns='zzDescription',
                                              values='Value').sort_values(by='LogTime')
        
        system_data = system_data.rename(columns={col_name:col_name.replace(' ', '') for col_name in system_data.columns})
        
        system_data.sort_values(by='LogTime')

        run_time_cols = list(set([col_name for col_name in system_data.columns 
                                                        if 'Time' in col_name 
                                                        or 'RunHours' in col_name
                                                        or 'Hour' in col_name]))

        print(f'Run Time Columns {run_time_cols}')

        try:
            assert len(run_time_cols) > 0
            run_time_col = run_time_cols[0]
            # system_data.rename({run_time_col:'RunHours'}, axis=1, inplace=True)

            print(f'Data for system {system_name} retrieved.')

            # Separate data by the swap number:

            run_hours = system_data[[run_time_col]][~system_data[run_time_col].isna()]
            run_hours_idx_list = list(run_hours.index)
            first_datum_idx = run_hours.index.min()
            first_datum_idx_num = run_hours_idx_list.index(first_datum_idx)
            swap_dates = [first_datum_idx]
            current_swap_num = 1

            run_hours['pump_num'] = current_swap_num
            for idx_num in range(first_datum_idx_num, len(run_hours.index)):
                if run_hours.iloc[idx_num][run_time_col] - run_hours.iloc[idx_num - 1][run_time_col] < -50:
                    current_swap_num += 1
                    swap_dates.append(run_hours.index[idx_num])

                run_hours.loc[run_hours.index[idx_num], 'pump_num'] = current_swap_num

            print(f'Swap dates for system {system_name} isolated.')

            all_systems_data[system_name] = {}
            
            all_systems_data[system_name]['swap_dates'] = swap_dates

            # Check if any swaps took place
            if len(swap_dates) == 0:
                all_systems_data[system_name]['pump_1'] = system_data
            
            elif len(swap_dates) == 1:
                all_systems_data[system_name]['pump_1'] = system_data[swap_dates[0]:]
            
            elif len(swap_dates) > 1:
                # Add the data, separated by swap number, into the system_data_all dictionary:
                for swap_dt_idx in range(len(swap_dates)):
                    pump_num = swap_dt_idx+1
                    swap_dt = swap_dates[swap_dt_idx]
                    if swap_dt_idx == len(swap_dates)-1:
                        all_systems_data[system_name][f"pump_{pump_num}"] = system_data[swap_dt:]
                    else:
                        next_swap_dt = swap_dates[swap_dt_idx+1]
                        all_systems_data[system_name][f"pump_{pump_num}"] = system_data[swap_dt:next_swap_dt]

            print(f'Data for system {system_name} partitioned by swap date.')

            print(f"Data for {system_name} prepared for plotting!")

        except:
            # raise ValueError
            print(f"The system {system_name} does not have any parameters that include the strings `RunHours` nor `Time`. Therefore, it must not be a pump. Skipping.")
            continue

    if len(all_systems_data) > 0:
        print(f"\n\nData retrieved and prepared for the following systems: ")
        for system in all_systems_data.keys():
            print(system)
    
    else:
        print('No data found for the specified systems.')
    
    return all_systems_data

all_systems_data = plot_prep_from_parquet(DATA_FILES_DIR)

## Create dashboards ##

def interactive_plot_system_data_1(data,
                                 save_dest,
                                 system,
                                 system_position,
                                 cols=None,
                                 save=True,
                                 show_plots=False):
    
    
    start_datetime = data.index.min().strftime('%d/%m/%y %H:%M:%S')
    
    system_name = f"{system_position}: {system.capitalize().replace('_', ' ')}, Start Date-Time: {start_datetime}"

    parameters_ordered = {'DryPump':[],
                          'Booster':[],
                          'ExhaustAndShaft':[],
                          'Time':[],
                          'Oil':[],
                          'Flow':[],
                          'Other':[]
                          }

    data = data.rename(columns={col:col.replace(' ', '').replace('DP', 'DryPump').replace('MB', 'Booster') for col in data.columns})

    for column in data.columns:
        if ('Dry' in column or 'DP' in column) and ('Hours' not in column and 'Time' not in column):
            parameters_ordered['DryPump'].append(column)
        elif 'Booster' in column or 'MB' in column:
            parameters_ordered['Booster'].append(column)
        elif 'Exhaust' in column or 'Shaft' in column:
            parameters_ordered['ExhaustAndShaft'].append(column)
        elif 'Oil' in column:
            parameters_ordered['Oil'].append(column)
        elif 'Flow' in column:
            parameters_ordered['Flow'].append(column)
        else:
            parameters_ordered['Other'].append(column)
    
    if cols==None:
        cols = data.columns
        # print(cols)
    
    fig_cols = ceil(len(cols)**0.5)
    
    fig_rows = ceil(len(cols)/fig_cols)
    
    parts_plots = {}
    count = 0
    for i in parameters_ordered.keys():
        # print(f'\npart parames before anything: \n{parameters_ordered}')
        # parameters_ordered[i] = [parameters_ordered[i][j] for j in parameters_ordered[i] if j in data.columns]
        # print(f'\npart params after checking against data columns: \n{parameters_ordered}')
        part_data = data[parameters_ordered[i]]
#         return part_data
        part_data = part_data.rename({col:col.replace(' ', '') for col in part_data.columns}, axis=1)
        parameters_ordered[i] = [param.replace(' ', '') for param in parameters_ordered[i]]
        parts_plots[i] = []
        # print(parameters_ordered)
        for j in range(len(parameters_ordered[i])):
            col_data = part_data[parameters_ordered[i][j]]
            # print(type(col_data))
            if col_data.notna().sum() < 1:
                continue
            elif col_data.notna().sum() < 15:
                radius_size=3
            else:
                radius_size=0.8
            source = plotting.ColumnDataSource(pd.DataFrame(col_data[col_data.notna()]))
            current_parameter = parameters_ordered[i][j]
            
            # Create interactive hovertool
            fig_hover_tool = HoverTool(tooltips=[('LogTime', '@LogTime{%Y-%m-%d %H:%M:%S.%3N}'),
                                                 (f'{current_parameter}', f'@{current_parameter}')],
                                       formatters={'@LogTime':'datetime'},
                                       mode='mouse')            
            if count > 0:
                fig = plotting.figure(x_axis_label='DateTime',
                                      y_axis_label=current_parameter,
                                      x_range=shared_x_range,
                                      x_axis_type='datetime',
                                      title=f"{current_parameter}")
                # print(f'\n\nData plotted for {system_position} {system} {i}:{j}')
            
            else:
                fig = plotting.figure(x_axis_label='DateTime',
                                      y_axis_label=current_parameter,
                                      x_axis_type='datetime',
                                      title=f"{current_parameter}")
                # print(f'\n\nData plotted for {system_position} {system} {i}:{j}')
            
            if count == 0:
                count += 1
                shared_x_range = fig.x_range

            fig.line(x='LogTime', 
                 y=current_parameter, 
                 source=source, 
#                  legend_label=cols[i]
                 color='#09ed28'
                )
            
            fig.circle(x='LogTime',
                       y=current_parameter,
                       source=source,
                       color='#f28a13',
                       radius=radius_size)
            
            fig.add_tools(fig_hover_tool)

            fig.xaxis.major_label_orientation = math.pi/4

            fig.axis.axis_label_text_font_size = '10px'

            parts_plots[i].append(fig)
    
    
    plot_title = Div(text=f"{system_name}") #, style={'font-size':'300%', 
                                            #       'color':'black', 
                                            #       'text-align':'center', 
                                            #       'margin':'auto'})
    
    plot_columns = []
    for key in parts_plots.keys():
        plot_columns.append(layouts.column(parts_plots[key]))

    if save:
        io.output_file(os.path.join(save_dest, 
                                    f"{system_position} {system}.html"))
        
        plotting.save(layouts.column(plot_title,
                                     layouts.row(plot_columns)))
        
    if show_plots:
        io.show(layouts.column(plot_title,
                                     layouts.row(plot_columns)))
        
bokeh_system_data_plotters = {'mk1':interactive_plot_system_data_1}

def interactive_plot_all_systems_data(all_systems_data,
                                      save_dest,
                                      mark=1,
                                      cols=None,
                                      save=True,
                                      show_plots=False):
    
    system_positions = [str(dict_key) for dict_key in all_systems_data.keys()]
    
    for position in system_positions:
        position_systems = [dict_key for dict_key in all_systems_data[position].keys() if 'swap_date' not in str(dict_key)]
        
        for system in position_systems:
            columns = all_systems_data[position][system].columns
            columns = {col:col.replace(' ', '').replace('DP', 'DryPump').replace('MB', 'Booster') for col in columns}
            all_systems_data[position][system].rename(columns={})
            print(f'\nWorking on {position} {system}.\n')
            bokeh_system_data_plotters[f"mk{mark}"](data=all_systems_data[position][system],
                                                    save_dest = save_dest,
                                                    system_position = f"{position}",
                                                    system=f"{system}",
                                                    show_plots=show_plots,
                                                    save=save)

            if save:
                text = '\033[1m' + f"Dashboard for" + f' {position} {system} ' + f"generated and placed at" + f" '{save_dest}.'" + '\033[0m'
                colored_text = colored(text=text, color='blue')
                print(colored_text)

interactive_plot_all_systems_data(all_systems_data,
                                  save_dest=FIG_DIR,
                                  mark=1,
                                  show_plots=False,
                                  save=True)
                                  