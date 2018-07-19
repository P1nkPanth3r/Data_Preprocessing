# Description:
# Program to process data and generate a few features for the Recruit_Restaurant_Visitor_Forecasting challenge on Kaggle.
# Much if not all of this program could be done in Pandas / other helper libraries - however, I found it educational to
# write this basic data processing program with minimal third-party package dependencies (I was also just getting into
# Pandas during this competition which would have made it somewhat more time consuming to write).

import os
import time
import datetime
from datetime import date
import operator

def data_import(cwd,filenames):
    print('Beginning Data Import...')
    start_time = time.time()
    os.chdir(cwd)
    data = []
    for filename in filenames:
        file = open(filename, 'rb').read().decode().split('\n')
        row = [line.split(',') for line in file]
        data.append(row)
    print('Data import complete.', '\n', 'Import duration (s) : ', time.time() - start_time)
    return data

def data_preprocess(datasets):
    print('Beginning Data Pre-Processing...')
    start_time = time.time()
    # Magic numbers for datasets to be used below
    air_reservations = 0
    hpg_reservations = 4
    air_hpg_map = 7

    # Clean up blanks at the end of datasets
    for dataset in range(len(datasets)):
        datasets[dataset] = datasets[dataset][:-1]

    # Add values from hpg system to air system using the provided air/hpg conversion dictionary
    search_dict = {}
    for row in datasets[air_hpg_map]:
        search_dict[row[1]] = row[0]
    for row in datasets[hpg_reservations][1:]:
        if row[0] in search_dict:
            row[0] = search_dict[row[0]]
            datasets[air_reservations].append(row)

    search_dict = {}
    for row in datasets[air_reservations][1:]: # All rows excluding columns headers
        row[1] = datetime.datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S').date() # Datetime which reservation is made for
        row[2] = datetime.datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S').date() # Datetime when reservation was made
        row[3] = int(row[3]) # Number of people reservation made for
        days_diff = (row[1] - row[2]).days

        key = row[0] + '_' + str(row[1]) + '_' + str(row[2]) # This key gives us a single unique entry for each date/time/restaurant
        if key in search_dict: # If reservation key already found, simply update the dictionary entry's count with new reservation info.
            search_dict[key][3] += row[3]
        else: # If no reservations found for this date/time/restaurant, create a new dictionary entry
            search_dict[key] = [row[0],row[1],row[2],row[3],days_diff]

    data = []
    for key, value in search_dict.items():
        data.append(search_dict[key])
    datasets[air_reservations][1:] = data
    print('Data preprocessing complete.', '\n', 'Preprocessing duration (s) : ', time.time() - start_time)
    return datasets

def factor_development(datasets):
    start_time = time.time()
    air_reservations = 0

    # Here we retrieve and develop several characteristics of interest to be used as features in our model.
    # The dataset is also squashed so for each air_store_id for each visit_datetime there is only one record.
    datasets[air_reservations][1:] = sorted(datasets[air_reservations][1:], key=operator.itemgetter(0, 1))
    air_reservations_sorted = datasets[air_reservations][1:]
    data = [["store_id","datetime","total_reservations","reserve_weighted_average_days",
             "day_of_month","month","is_friday","is_saturday","is_sunday"]]
    item = 0
    while item < len(air_reservations_sorted):
        item_id = air_reservations_sorted[item][0]
        item_datetime = air_reservations_sorted[item][1]

        id_time = item_id + '_' + str(item_datetime)
        cur_id_time = id_time
        id_time_values = []
        while cur_id_time==id_time and item < len(air_reservations_sorted):
            id_time_values.append([air_reservations_sorted[item][3],air_reservations_sorted[item][4]])
            item += 1
            if item == len(air_reservations_sorted):
                cur_id_time = air_reservations_sorted[item-1][0] + '_' + str(air_reservations_sorted[item-1][1])
            else:
                cur_id_time = air_reservations_sorted[item][0] + '_' + str(air_reservations_sorted[item][1])

        total_reservations = float(sum([x[0] for x in id_time_values]))
        avg_reserve_visit = float(  [a*b for a,b in zip([x[0] for x in id_time_values], # Average reservation size
                                                        [y[1] for y in id_time_values])
                                    ][0]
                                  ) / total_reservations
        day_of_month = item_datetime.day
        month = item_datetime.month
        day_of_week = date.weekday(item_datetime)
        # Create dummy variables for Friday - Sunday (we might expect different behavior on weekends than weekdays).
        if day_of_week == 4:
            bln_friday = 1
            bln_saturday = 0
            bln_sunday = 0
        elif day_of_week == 5:
            bln_friday = 0
            bln_saturday = 1
            bln_sunday = 0
        elif day_of_week == 6:
            bln_friday = 0
            bln_saturday = 0
            bln_sunday = 1
        else:
            bln_friday = 0
            bln_saturday = 0
            bln_sunday = 0

        data.append([item_id,
                     item_datetime,
                     total_reservations,
                     avg_reserve_visit,
                     day_of_month,
                     month,
                     bln_friday,
                     bln_saturday,
                     bln_sunday])
        item += 1

    print('Factor development complete.', '\n', 'Development duration (s) : ', time.time() - start_time)
    return data

# MAIN PROGRAM
if __name__=='__main__':
    cwd = r'..\kaggle_data\Competitions\Recruit_Restaurant_Visitor_Forecasting\data_raw'
    filenames = ['air_reserve.csv','air_store_info.csv','air_visit_data.csv','date_info.csv',
                 'hpg_reserve.csv','hpg_store_info.csv','sample_submission.csv','store_id_relation.csv']
    datasets = data_import(cwd,filenames)
    datasets = data_preprocess(datasets)
    datasets = factor_development(datasets)

print('Program Complete')
