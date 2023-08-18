from datetime import datetime, timedelta
from multiprocessing import Pool, Queue

import pandas as pd
import glob
import os
import xml.etree.ElementTree as ET


def get_meter_num_and_channel(row_channel_):
    try:
        meter_num, channel_id = list(row_channel_.values())[0].split(':')
    except Exception as ex:
        print(f'Error in {row_channel_}')
        return None, None
    return meter_num, channel_id


def get_dataframe(path_file):
    root = ET.parse(path_file).getroot()
    pandas_rows = []
    for i, channel_el in enumerate(root.findall('Channels')[0].findall('Channel')):
        row_channel = channel_el.attrib.copy()
        row_channel_id = channel_el.find('ChannelID').attrib.copy()
        meter_num, channel_id = get_meter_num_and_channel(row_channel_id)
        row_channel['meter_num'] = meter_num
        row_channel['channel_id'] = channel_id

        if meter_num is None:
            print(f'No meter number found file {path_file} elements {row_channel_id}')
        try:
            sanity_check = False
            row_time_period = {}
            start_time = None
            end_time = None
            curr_time = None
            if channel_el.find('ContiguousIntervalSets') is None:
                interval_element = channel_el  # No interval element, so the readings are at the same level of channelID
            else:
                interval_element = channel_el.find('ContiguousIntervalSets').find('ContiguousIntervalSet')
                row_number_readings = interval_element.attrib.copy()  # For sanity check
                sanity_check = 'NumberOfReadings' in row_number_readings
                if interval_element.find('TimePeriod') is not None:
                    row_time_period = interval_element.find('TimePeriod').attrib
                    start_time = datetime.strptime(row_time_period['StartTime'], '%Y-%m-%dT%H:%M:%SZ')
                    curr_time = start_time
                    end_time = datetime.strptime(row_time_period['EndTime'], '%Y-%m-%dT%H:%M:%SZ')

            interval_minutes = int(row_channel['IntervalLength'])

            row_add = []
            row_dict = row_channel.copy() # {**row_channel, **row_channel_id}
            # row_dict = {**row_dict, **row_time_period}
            for reading_element in interval_element.find('Readings').findall('Reading'):
                dict_row = {**row_dict, **reading_element.attrib}

                if curr_time is not None:
                    # ReadingTime <- Should be from TimePeriod StartTime="2023-03-14T04:00:00Z" + Interval (in minutes) EndTime="2023-03-15T04:00:00Z"
                    curr_time += timedelta(minutes=interval_minutes)
                    dict_row['ReadingTime'] = curr_time.strftime('%Y-%m-%dT%H:%M:%SZ')

                row_add.append(
                    dict_row
                )

            if sanity_check:
                rows_to_add = len(row_add)
                number_readings = int(row_number_readings['NumberOfReadings'])
                assert rows_to_add == number_readings, f'Error in the sanity check for {row_channel_id} rows to add {rows_to_add} number to add {number_readings}'

            pandas_rows.extend(row_add)
        except Exception as ex:
            print(f'Error in {path_file}')
    return pd.DataFrame(pandas_rows)


def save_all_files_parallel(files_path_, batch_size=100, max_rows=30_000_000):
    with Pool(os.cpu_count()) as pool:
    # with Pool(1) as pool:
        for i in range(0, len(files_path_), batch_size):
            result = pool.map(get_dataframe, files_path_[i:i + batch_size])
            dfs_ = [df for df in result]
            del result
            print(f'Saving AMI, file # {i}')
            df_ = pd.concat(dfs_)
            del dfs_
            for c1, c in enumerate(range(0, len(df_), max_rows)):
                df_[c:c + max_rows].to_parquet(os.path.join(output_folder, f'AMI-{i}-{c1}.parquet'))

    print(f'finished')


if __name__ == '__main__':
    folder_xmls = 'C:\\Users\sqyan\Documents\Customer Data - AMI\MeterReadingSyncSave\*.xml'
    output_folder = 'C:\\Users\sqyan\Documents\Customer Data - AMI\MeterReadingSyncSave_output'
    # output_folder = 'C:\\Users\sqyan\\NTT DATA EMEAL\IEDR - Documents\IEDR - IT\\000 - Data Specifications (NYSERDA)\Customer Data\Sample Data Delivery August 1st 2023\MeterReadingSyncSave_manual_output_new_'
    files_path = glob.glob(folder_xmls)
    save_all_files_parallel(files_path, batch_size=400)