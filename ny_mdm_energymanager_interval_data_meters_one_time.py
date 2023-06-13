# Importing the required libraries
import xml.etree.cElementTree as Xet # For parsing XML element tree
import pandas as pd # For loading the list of data into .tsv easily
from datetime import timedelta, datetime # For date/time formatting
import sys # For file management
import hashlib # For getting the md5 checksum of a file
import os # For file management
import time as _time # For tracking runtime
import pytz # For timezones
import logging

import csv

# Sets the correct timezone for the interval times.
def dt_parse(t):
    ret = pytz.timezone('UTC').localize(datetime.strptime(t[0:18],'%Y-%m-%dT%H:%M:%S'))
    return ret

# Start counter to record runtime.
start = _time.time()


#Create hash tables
'''
hash_meter_ids ={}
counter = 0
with open("NY_meters.csv","r") as file:
    reader = csv.reader(file,lineterminator="\n",delimiter=",")
    for row in reader:
        if counter == 0:
            hash_meter_ids[hash(str(row[0]).replace("'",""))] = str(row[0]).replace("'","")
            counter += 1
        else:
            hash_meter_ids[hash(str(row[0]).replace("'",""))] = str(row[0]).replace("'","")
'''

# Interval data file column headers. Comes from Uplight spec.
cols = ["service_point_id", "meter_id", "timestamp", "timezone", "interval_value", "interval_units", "usage", "energy_type", "energy_units", "energy_direction", "is_estimate", "is_outage", "channel_id", "is_deleted", "update_datetime"]
rows = []
error_channel_id_rows = []
error_meter_id_rows = []

# Directories for input/output files.
#input_path = '\\\clornas01\\Uplight\\NY\\in'
#output_path = '\\\clornas01\\Uplight\\NY\\out\\test_results'
input_path = "C:\\Users\\U355445\\Documents\\AMI Filter\\NY_in"
output_path = "C:\\Users\\U355445\\Documents\\AMI Filter\\test"
error_path = '\\\clornas01\\Uplight\\NY\\errors'
log_path = '\\\clornas01\\Uplight\\NY\\logs'
log_filename = os.path.join(log_path, 'avangrid-energymanager-nyseg_intervalusage_uat_log_' + datetime.now().strftime("%Y%m%d-%H%M%S%f")[:-3] + '.log')

#date_folders = ['2023-04-19']

# Turn on logging
logging.basicConfig(filename=log_filename, filemode="w", level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %H:%M:%S')
# Send first logging message that program started.
logging.info('NY interval data transformation program has started.')

# Manifest file column headers.
cols_manifest = ["file_name", "file_size", "md5_checksum", "line_count"]
rows_manifest = []
# Declare manifest file name and add headers to the file. Output to the log file that the manifest file has been created.
manifest_file_name = 'avangrid-energymanager-nyseg_manifestmdm_uat_' + datetime.now().strftime("%Y%m%d-%H%M%S%f")[:-3] + '.tsv'
df_manifest = pd.DataFrame(rows_manifest, columns=cols_manifest)
df_manifest.to_csv(os.path.join(output_path, manifest_file_name), header=True, index=False, sep ='\t', lineterminator='\r\n')
logging.info(manifest_file_name + ' created.')
logging.info('------------------------------------------------------')

# Declare initial tsv filename. A new tsv filename will be given any time a file reaches 10 million rows.
tsv_filename = 'avangrid-energymanager-nyseg_intervalusage_uat_' + datetime.now().strftime("%Y%m%d-%H%M%S%f")[:-3] + '.tsv'
total_rows = 0 # This will represent count of rows so the script knows when to break to a new file.
header = True # This flag is true until the headers are written to the file.
    
# Iterate through all xml files from the source "in" folder.
count = 0
#for date in date_folders:
#    logging.info(date + ' - Folder has been opened.')
#    temp_path = os.path.join(input_path, date)
for filename in os.listdir(input_path):
    # If the file found in the directory is not .xml, skip it.
    if not filename.endswith('.xml'): continue
    logging.info(filename + ' - Processing has started.')
    
    # Error save flags. True means the errors will output to files. Outputting errors increases run time - recommended to kee on false. The log file will capture the error at a high level.
    save_error_channel_id = False # Keep this false. Turning this on introduces memory issues.
    save_error_meter_id = False # Keep this false. Turning this on introduces memory issues.
    # Set up error file names, should they need to be created. Each source .xml file would get its own error file.
    error_channel_id_file_name = filename.split('.')[0] + '_ERRORS_CHANNEL_ID_MAPPING_' + datetime.now().strftime("%Y%m%d-%H%M%S%f")[:-3] + '.tsv'
    error_channel_header = True # This flag is true until the headers are written to the file.
    error_meter_id_file_name = filename.split('.')[0] + '_ERRORS_NO_METER_ID_' + datetime.now().strftime("%Y%m%d-%H%M%S%f")[:-3] + '.tsv'
    error_meter_header = True # This flag is true until the headers are written to the file.
    
    fullname = os.path.join(input_path, filename) # Filename including the folder path.
    
    ################
    # get an iterable of the xml tree.
    context = Xet.iterparse(fullname, events=("start", "end"))

    path = [] # Will hold the XML tree path as elements are iterated over
    status_code_is_estimate = [] # Record the statuses that represent estimated intervals.
    status_code_is_outage = [] # Record the statuses that represent outage intervals.

    isRegister_file = False # This flag is used to detect register files so that they can be skipped.
    for event, elem in context: # Iterate over the XML tree.
        if event == 'start': # Event represents the start and end of a tree element.
            path.append(elem.tag) # Each new element is added to a variable to keep track of the script's spot in the tree.
            if elem.tag == 'ReadingStatusRef':
                status_code = elem.attrib['Ref'] # Status codes may represent estimate or outage intervals.
            if elem.tag == 'Channel': # Get energy type of channel.
                if elem.attrib['MarketType'] == 'Electric':
                    energy_type = 'E'
                elif elem.attrib['MarketType'] == 'Gas':
                    energy_type = 'G'
                elif elem.attrib['MarketType'] == 'Water':
                    energy_type = 'W'
                else:
                    energy_type = 'E'

                # If the file is a Register file, skip and do not write any data from it.
                #if elem.attrib['IsRegister'] == 'true':
                #    isRegister_file = True
                #    break

                interval_value = elem.attrib['IntervalLength']
                interval_units = 'minute'
        elif event == "end": # This happens at the end of each XML tree element.
            if 'Code' in path: # Get all status codes and record the mapping to be used for the remainder of the file.
                if elem.text.startswith('EST'):
                    status_code_is_estimate.append(status_code) if status_code not in status_code_is_estimate else status_code_is_estimate
                elif elem.text.startswith('POWER'):
                    status_code_is_outage.append(status_code) if status_code not in status_code_is_outage else status_code_is_outage
            
            if 'Creation_Datetime' in path:
                updated_datetime = dt_parse(elem.attrib['Datetime'])
            
            if 'ChannelID' in path: # Get meter ID and channel details.
                error = False
                if 'ServicePointChannelID' in elem.attrib: # If Service Point ID is provided in the XML, mapping from Service Point ID to Meter ID will be required.
                    #service_point_id = elem.attrib['ServicePointChannelID'].split(':')[0]
                    meter_id = elem.attrib['ServicePointChannelID'].split(':')[0]
                    channel_id = int(elem.attrib['ServicePointChannelID'].split(':')[1])
                    #try:
                    #    meter_id = hash_meter_ids[hash(service_point_id)]
                    #except KeyError:
                    #    logging.info('Service Point ID ' + service_point_id + ' not found to get meter ID.')
                    #    meter_id = None
                elif 'IntervalChannelID' in elem.attrib: # Get the meter ID.
                    meter_id = elem.attrib['IntervalChannelID'].split(':')[0]
                    channel_id = int(elem.attrib['IntervalChannelID'].split(':')[1])
                else:
                    error = 'service point or meter ID not in XML file' # This error message will prevent the record from saving.
                
                # Energy direction mapping.
                if channel_id == 1 or channel_id == 101:
                    energy_units = 'kWh'
                    energy_direction = 'delivered'
                    channel_id = 'usage_delivered'
                elif channel_id == 2 or channel_id == 102:
                    energy_units = 'kWh'
                    energy_direction = 'received'
                    channel_id = 'usage_received'
                else:
                    # If the channel ID is not mapped, output the ID in the log file for future investigation, and don't save any data.
                    error = 'channel_mapping_missing' # This error message will prevent the record from saving.
                    logging.info('Channel ID = ' + str(channel_id) + ' is not mapped.')
                    energy_units = None
                    energy_direction = None


            if 'TimePeriod' in path: # Get timestamp.
                timestamp_datetime = dt_parse(elem.attrib['StartTime'])

            # Reading is where the interval usage is found.
            if 'Reading' in path: 
                #Change here 5/25: multiplier
                #usage = elem.attrib['Value']
                usage = float(elem.attrib['Value']) *  .001
                is_estimate = 'true' if elem.attrib['StatusRef'] in status_code_is_estimate else 'false'
                is_outage = 'true' if elem.attrib['StatusRef'] in status_code_is_outage else 'false'

                # To get the interval's timestamp, add the interval length to the previous interval's timestamp.
                # Timestamp is converted to Eastern Time.
                timestamp_datetime = timestamp_datetime.astimezone(pytz.timezone('US/Eastern')) + timedelta(minutes=int(interval_value))
                timestamp_save = timestamp_datetime.astimezone(pytz.timezone('US/Eastern')) # Changes timezone if daylight savings is crossed.
                timezone = datetime.strftime(timestamp_save, "%Z")

                # Create row of data for this reading/interval to be added to the .tsv file.
                row_data = {"service_point_id": meter_id,
                    "meter_id": meter_id,
                    "timestamp": datetime.strftime(timestamp_save, "%Y-%m-%dT%H:%M:%S.000%z").replace('-0400', '-04:00').replace('-0500', '-05:00'),
                    "timezone": timezone,
                    "interval_value": interval_value,
                    "interval_units":interval_units,
                    "usage":usage,
                    "energy_type":energy_type,
                    "energy_units":energy_units,
                    "energy_direction":energy_direction,
                    "is_estimate":is_estimate,
                    "is_outage":is_outage,
                    "channel_id":channel_id,
                    "is_deleted":"false",
                    "update_datetime":datetime.strftime(updated_datetime.astimezone(pytz.timezone('US/Eastern')), "%Y-%m-%dT%H:%M:%S.000%z").replace('-0400', '-04:00').replace('-0500', '-05:00'),
                }

                # Error Handling
                # If channel mapping is missing, nothing is saved (by default), and the channel ID is outputted to the log file.
                if error == 'channel_mapping_missing':
                    if save_error_channel_id:
                        error_channel_id_rows.append(row_data)
                    else:
                        pass
                elif error == 'service point or meter ID not in XML file':
                    pass
                else:
                    if meter_id:
                        rows.append(row_data)
                    # If the service point is not found in the manually provided meter ID mapping, save the records to an
                    # error file with just the service point ID populated (no meter ID).
                    else:
                        row_data['service_point_id'] = service_point_id # Since no meter ID is found, populate the service point ID in the error file for investigation.
                        if save_error_meter_id: # Error records are only saved if the flag is set to True at the start. Recommended to not save errors. Log file will capture them.
                            error_meter_id_rows.append(row_data)
                        else:
                            pass
                
                # Once 400,000 rows are recorded, write them to the .tsv file and clear the list.
                # Repeat adding 400,000 rows to the file until there are no more intervals.
                if len(rows) >= 400000:
                    logging.info(str(len(rows)) + ' records saved. Runtime: ' + str(_time.time() - start) + ' seconds') # Output run time to log.
                    
                    # Declare data rows to be written to the file.
                    df = pd.DataFrame(rows, columns=cols)
                    #Drop '600..' meter ids as they are service point ids
                    df.drop(df[str(df['meter_id']).startswith('600')].index)
                    # Appends the data rows to the tsv file.
                    df.to_csv(os.path.join(output_path, tsv_filename), index=False, sep ='\t', header=header, mode='a', lineterminator='\n')
                    # Header flag turns false after the first iteration.
                    header = False
                    
                    # Record the total number of rows before clearing the list.
                    total_rows += len(rows)
                    rows = []
                    
                    # Load errors into error files (if flags are turned on).
                    if len(error_channel_id_rows) > 0:
                        df_error_channel_id = pd.DataFrame(error_channel_id_rows, columns=cols)
                        df_error_channel_id.to_csv(os.path.join(error_path, error_channel_id_file_name), header=error_channel_header, index=False, sep ='\t', mode='a', lineterminator='\n')
                        error_channel_header = False
                        error_channel_id_rows = []
                    if len(error_meter_id_rows) > 0:
                        df_error_meter_id = pd.DataFrame(error_meter_id_rows, columns=cols)
                        df_error_meter_id.to_csv(os.path.join(error_path, error_meter_id_file_name), header=error_meter_header, index=False, sep ='\t', mode='a', lineterminator='\n')
                        error_meter_header = False
                        error_meter_id_rows = []
                    
                    

                    # Once the tsv file has over 10,000,000 records, stop writing to this file, record the file details to the manifest, and start a new tsv file.
                    if total_rows >= 10000000:
                        # Create row with file details to be written to manifest.
                        rows_manifest.append({
                            "file_name":tsv_filename,
                            "file_size":os.path.getsize(os.path.join(output_path, tsv_filename)),
                            "md5_checksum":hashlib.md5(open(os.path.join(output_path, tsv_filename),'rb').read()).hexdigest(),
                            "line_count":total_rows
                        })
                        logging.info(tsv_filename + ' was generated successfully.')

                        # With the .tsv file generated, open it up and remove the blank line at the bottom.
                        with open(os.path.join(output_path, tsv_filename), 'rb+') as filehandle:
                            filehandle.seek(-2, os.SEEK_END)
                            filehandle.truncate()
                        
                        # Append row with file details to manifest file.
                        df_manifest = pd.DataFrame(rows_manifest, columns=cols_manifest)
                        df_manifest.to_csv(os.path.join(output_path, manifest_file_name), header=False, index=False, sep ='\t', mode='a', lineterminator='\n')
                        rows_manifest = []

                        # Set the file name of the next .tsv file to be generated.
                        tsv_filename = 'avangrid-energymanager-nyseg_intervalusage_uat_' + datetime.now().strftime("%Y%m%d-%H%M%S%f")[:-3] + '.tsv'
                        total_rows = 0
                        # Reset header flag so that the header is written to the next file on the first iteration.
                        header = True
                
            elem.clear() # Clears the XML elements to save memory.
            path.pop() # Remove the ending element from the XML tree path.
    ###################

    # If there are remaining rows to be written to the file, write them to the file.
    if len(rows) > 0:
        # This section of code writes the final group of rows before moving on to the next XML file.
        logging.info(str(len(rows)) + ' records saved. Runtime: ' + str(_time.time() - start) + ' seconds')

        # Declare data rows to be written to the file.
        df = pd.DataFrame(rows, columns=cols)
        #Remove '600' ids as they are service point ids
        #df.drop(df[str(df['meter_id']).startswith('600')].index)
        # Appends the data rows to the tsv file.
        df.to_csv(os.path.join(output_path, tsv_filename), index=False, sep ='\t', header=header, mode='a', lineterminator='\n')
        # Header flag turns false after the first iteration.
        header = False
        
        # Record the total number of rows before clearing the list.
        total_rows += len(rows)
        rows = []
        
        if total_rows >= 10000000:
            # Create row with file details to be written to manifest.
            rows_manifest.append({
                "file_name":tsv_filename,
                "file_size":os.path.getsize(os.path.join(output_path, tsv_filename)),
                "md5_checksum":hashlib.md5(open(os.path.join(output_path, tsv_filename),'rb').read()).hexdigest(),
                "line_count":total_rows
            })
            logging.info(tsv_filename + ' was generated successfully.') # Output to the log confirming the tsv file is complete.

            # With the .tsv file generated, open it up and remove the blank line at the bottom.
            with open(os.path.join(output_path, tsv_filename), 'rb+') as filehandle:
                filehandle.seek(-2, os.SEEK_END)
                filehandle.truncate()
            
            # Append row with file details to manifest file.
            df_manifest = pd.DataFrame(rows_manifest, columns=cols_manifest)
            df_manifest.to_csv(os.path.join(output_path, manifest_file_name), header=False, index=False, sep ='\t', mode='a', lineterminator='\n')
            rows_manifest = []

            # Set the file name of the next .tsv file to be generated.
            tsv_filename = 'avangrid-energymanager-nyseg_intervalusage_uat_' + datetime.now().strftime("%Y%m%d-%H%M%S%f")[:-3] + '.tsv'
            total_rows = 0
            # Reset header flag so that the header is written to the next file on the first iteration.
            header = True
    
    # Load errors into error files (if desired).
    if len(error_channel_id_rows) > 0:
        df_error_channel_id = pd.DataFrame(error_channel_id_rows, columns=cols)
        df_error_channel_id.to_csv(os.path.join(error_path, error_channel_id_file_name), header=error_channel_header, index=False, sep ='\t', mode='a', lineterminator='\n')
        error_channel_id_rows = []
    if len(error_meter_id_rows) > 0:
        df_error_meter_id = pd.DataFrame(error_meter_id_rows, columns=cols)
        df_error_meter_id.to_csv(os.path.join(error_path, error_meter_id_file_name), header=error_meter_header, index=False, sep ='\t', mode='a', lineterminator='\n')
        error_meter_id_rows = []
    
    # If the file is seen to be a register file, skip it and record that to the log. Or for a regular file, confirm completion in the log.
    if isRegister_file:
        logging.info(filename + ' - Skipped because this is a register file.')
    else:
        logging.info(filename + ' - Processed successfully.')
    logging.info('------------------------------------------------------')


# If a last file is created, write the details to the manifest file.
if total_rows > 0:
    # With the .tsv file generated, open it up and remove the blank line at the bottom.
    with open(os.path.join(output_path, tsv_filename), 'rb+') as filehandle:
        filehandle.seek(-2, os.SEEK_END)
        filehandle.truncate()

    # Record the file details for the manifest.
    rows_manifest.append({
        "file_name":tsv_filename,
        "file_size":os.path.getsize(os.path.join(output_path, tsv_filename)),
        "md5_checksum":hashlib.md5(open(os.path.join(output_path, tsv_filename),'rb').read()).hexdigest(),
        "line_count":total_rows
    })
    logging.info(tsv_filename + ' was generated successfully.')
    
    # Append file details to manifest file.
    df_manifest = pd.DataFrame(rows_manifest, columns=cols_manifest)
    df_manifest.to_csv(os.path.join(output_path, manifest_file_name), header=False, index=False, sep ='\t', mode='a', lineterminator='\n')
    rows_manifest = []


# The code below opens the manifest file and removes the new line characters at the end so that there's no blank line at the bottom.
with open(os.path.join(output_path, manifest_file_name), 'rb+') as filehandle:
    filehandle.seek(-2, os.SEEK_END)
    filehandle.truncate()
logging.info(manifest_file_name + ' was completed successfully.')

logging.info('Total Runtime: ' + str(_time.time() - start) + ' seconds')