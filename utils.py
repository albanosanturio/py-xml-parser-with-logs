import pandas as pd
import os
import sys

# NTT:
# Function validates the arguments provided
# > python script.py NYSEG      runs for nyseg meters
# > python script.py RGE        runs for rge meters
# > python script.py ALL        runs for all meters
def parse_opco():
    arg_list = sys.argv
    opco_list = ["NYSEG", "RGE", "ALL"]

    if len(arg_list) < 2:
        print("Not enough arguments, try: \n>python script.py NYSEG \n>python script.py RGE \n>python script.py ALL")
        exit()
    if len(arg_list) > 2:
        print("Too many arguments, try: \n>python script.py NYSEG \n>python script.py RGE \n>python script.py ALL")
        exit()
    if arg_list[1] not in opco_list:
        print("Wrong OPCO arguments, try: \n>python script.py NYSEG \n>python script.py RGE \n>python script.py ALL")
        exit()
    return arg_list[1]


# NTT:
# Function takes opco and the path to the approved meters csv
# Returns a filtered list of meters
def approved_meters(opco: str, meters_path: str):
    df_approved_meters = pd.read_csv(meters_path)
    df_approved_meters["Meterid"] = df_approved_meters["Meterid"].astype("string")
    if opco == "ALL":
        selected_ids = df_approved_meters["Meterid"]
    else:
        selected_ids = (df_approved_meters.loc[df_approved_meters["OPCO"] == opco])["Meterid"]
    return set(selected_ids)


# NTT:
# Function takes opco and the path to the approved meters csv
# Returns a filtered list of meters
def approved_meters2(opco: str, meters_path: str):
    meters_df = pd.DataFrame(columns=[])
    for filename in os.listdir(meters_path):
        if not filename.endswith('.csv'): continue
        if opco == 'ALL':
            print("All files included: "+filename)
            df_temp = pd.read_csv(os.path.join(meters_path, filename), dtype=str)
            df_temp_subs = df_temp[["Meterid","ApprovedDate"]]
            meters_df = pd.concat([meters_df,df_temp_subs],ignore_index=True)
        elif opco in filename: 
            print(opco +" files included: "+filename)
            df_temp = pd.read_csv(os.path.join(meters_path, filename),dtype=str)
            df_temp_subs = df_temp[["Meterid","ApprovedDate"]]
            meters_df = pd.concat([meters_df,df_temp_subs],ignore_index=True)
        else: continue
    return(meters_df)
    

"""  
if 'TimePeriod' in path: # Get timestamp.
timestamp_datetime = dt_parse(elem.attrib['StartTime'])
timestamp_datetime = timestamp_datetime.astimezone(pytz.timezone('US/Eastern')) + timedelta(minutes=int(interval_value))
timestamp_save = timestamp_datetime.astimezone(pytz.timezone('US/Eastern')) # Changes timezone if daylight savings is crossed.
timezone = datetime.strftime(timestamp_save, "%Z")
 
 """
