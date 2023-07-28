from multiprocessing import Pool

import pandas as pd
import glob
import os
import xml.etree.ElementTree as ET


def get_dataframe(path_file):
    root = ET.parse(path_file).getroot()
    pandas_rows = []
    for i, channel_el in enumerate(root.findall('Channels')[0].findall('Channel')):
        row_channel = channel_el.attrib.copy()
        row_channel_id = channel_el.find('ChannelID').attrib.copy()
        try:
            sanity_check = False
            if channel_el.find('ContiguousIntervalSets') is None:
                row_time_period = {}
                interval_element = channel_el # No interval element, so the readings are at the same level of channelID
            else:
                interval_element = channel_el.find('ContiguousIntervalSets').find('ContiguousIntervalSet')
                row_number_readings = interval_element.attrib.copy()  # For sanity check
                sanity_check = 'NumberOfReadings' in row_number_readings
                if interval_element.find('TimePeriod') is not None:
                    row_time_period = interval_element.find('TimePeriod').attrib

            row_add = []
            row_dict = {**row_channel, **row_channel_id}
            row_dict = {**row_dict, **row_time_period}
            for reading_element in interval_element.find('Readings').findall('Reading'):
                row_add.append(
                    {**row_dict, **reading_element.attrib}
                )

            if sanity_check:
                rows_to_add = len(row_add)
                number_readings = int(row_number_readings['NumberOfReadings'])
                assert rows_to_add == number_readings, f'Error in the sanity check for {row_channel_id} rows to add {rows_to_add} number to add {number_readings}'

            pandas_rows.extend(row_add)
        except Exception:
            print(f'Error in {path_file}')
    return pd.DataFrame(pandas_rows)


def save_all_files_parallel(files_path_, chunk_size=100, max_rows=3_000_000):
    with Pool(os.cpu_count()) as pool:
        for i in range(0, len(files_path_), chunk_size):
            result = pool.map(get_dataframe, files_path_[i:i + chunk_size])
            dfs_ = [df for df in result]
            print(f'Saving AMI, file # {i}')
            df_ = pd.concat(dfs_)
            for c1, c in enumerate(range(0, len(df_), max_rows)):
                df_[c:c + max_rows].to_parquet(os.path.join(output_folder, f'AMI-{i}-{c1}.parquet'))

    print(f'finished')


if __name__ == '__main__':
    folder_xmls = 'C:\\Users\sqyan\Documents\Customer Data - AMI\MeterReadingSyncSave\*.xml'
    output_folder = 'C:\\Users\sqyan\Documents\Customer Data - AMI\MeterReadingSyncSave_manual_output'
    files_path = glob.glob(folder_xmls)
    save_all_files_parallel(files_path, chunk_size=400)
