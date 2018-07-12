import os
import time
import datetime

def data_import(cwd,filenames):
    print('Beginning Data Import...')
    import_start_time = time.time()
    os.chdir(cwd)
    data_raw = []

    for filename in filenames:
        file = open(filename, 'rb').read().decode().split('\n')
        lst_raw = [row.split(',') for row in file]
        data_raw.append(lst_raw)

    print('Data import complete.', '\n', 'Import duration (s) : ', time.time() - import_start_time)
    return data_raw

def data_preprocess(datasets):
    print('Beginning Data Pre-Processing...')
    preprocess_start_time = time.time()
    # Magic numbers for datasets to be used below
    air_data = 0
    hpg_data = 4
    airhpg_dict = 7
    # Clean up blanks at end of datasets
    for dataset in datasets:
        datasets[dataset] = datasets[dataset][:-1]

    # Add values from hpg system to air system using the provided air/hpg conversion dictionary
    search_dict = {}
    found_list = []
    for row in datasets[airhpg_dict]:
        search_dict[row[1]] = row[0]
    for row in datasets[hpg_data][1:]:
        if row[0] in search_dict:
            row[0] = search_dict[row[0]]
            datasets[air_data].append(row)

    search_dict = {}
    for row in datasets[air_data][1:]:
        row[1] = datetime.datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S').date()
        row[2] = datetime.datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S').date()
        row[3] = int(row[3])

        key = row[0] + '_' + str(row[1]) + '_' + str(row[2])
        if key in search_dict:
            search_dict[key][3] = search_dict[key][3] + row[3]
        else:
            search_dict[key] = [row[0],row[1],row[2],row[3],datediff]
    list_aggregate = []
    for key, value in search_dict.items():
        list_aggregate.append(search_dict[key])
    datasets[air_data][1:] = list_aggregate

    print('Data pre-processing complete.', '\n', 'Pre-processing duration (s) : ',
          time.time() - preprocess_start_time)
    return datasets

if __name__=='__main__':
    cwd = r'..\kaggle_data\Competitions\Recruit_Restaurant_Visitor_Forecasting\data_raw'
    filenames = ['air_reserve.csv','air_store_info.csv','air_visit_data.csv','date_info.csv',
                 'hpg_reserve.csv','hpg_store_info.csv','sample_submission.csv','store_id_relation.csv']
    data_raw = data_import(cwd,filenames)
    datasets = data_preprocess(data_raw)
    for row in datasets[0][0:4]:
        print(row)
    print("...")
    print(datasets[0][-1])