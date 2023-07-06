# py-xml-parser-with-logs

#### Overview:
Script processes interval data read by energy meters.
Main goal is to parse the .xml input format to a .tsv needed
It's also needed to filter out some unneeded readings:
    - Filter out meters not included in an "approved meters" list
    - Filter out readings based on the "approval date" of the meters (Reading time must be AFTER approval date)
    - Process only meters of the selected company (opco)

#### Requirements:


##### Config.json file
In the same folder as the script there must be a .json config file containing the paths used (input, output, errors, logs, approved ids)

{
    "paths": {
        "input_path": ".\\in",
        "output_path": ".\\out",
        "error_path": ".\\errors",
        "log_path": ".\\logs",
        "approved_ids_path": ".\\approved_ids"
    }
}

##### Approved ids files
In the approved_ids_path there must be .csv files containing the meters that are ok to be processed.
Files have to be separated by opco, which has to be in the name.
There may be more that one file for each opco. 
The script will pick up only the files for the opco selected

Sample files in path:
approved_ids_NYSEG_part1.csv
approved_ids_NYSEG_part2.csv
approved_ids_RGE_part1.csv
approved_ids_RGE_part2.csv
approved_ids_RGE_part3.csv


Each file is expected to have this format, at least the 'Meterid' and 'ApprovedDate' columns:

Contract Account,Meterid,CoCd,OPCO,ApprovedDate
xxxxxxxxxxx,102976527,1000,RGE,2023-04-05
xxxxxxxxxxx,102976573,1000,RGE,2023-04-05

##### Arguments
To run the script you have to pass 1 argument selecting opco:
>python script.py NYSEG    
>python script.py RGE      
>python script.py ALL  


#### Sample .XML input

 <Channels>
    <Channel IsRegister="false" MarketType="Electric" IntervalLength="15" NumberOfDials="-1" PulseMultiplier="-1" PressureCompensationFactor="-1" IsReadingDecoded="true" ReadingsInPulse="false">
      <ChannelID IntervalChannelID="102976525:1" />
      <ContiguousIntervalSets>
        <ContiguousIntervalSet NumberOfReadings="4">
          <TimePeriod StartTime="2023-04-04T04:00:00Z" EndTime="2023-04-05T04:00:00Z" />
          <Readings>
            <Reading Value="5" StatusRef="58" />
            <Reading Value="4" StatusRef="58" />
            <Reading Value="4" StatusRef="58" />
            <Reading Value="5" StatusRef="58" />
          </Readings>
        </ContiguousIntervalSet>
      </ContiguousIntervalSets>
    </Channel>
 </Channels>


 #### Sample .TSV output:
service_point_id	meter_id	timestamp	timezone	interval_value	interval_units	usage	energy_type	energy_units	energy_direction	is_estimate	is_outage	channel_id	is_deleted	update_datetime
102976525	102976525	2023-04-04T00:15:00.000-04:00	EDT	15	minute	0.005	E	kWh	delivered	false	false	usage_delivered	false	2023-04-05T03:01:02.000-04:00
102976525	102976525	2023-04-04T00:30:00.000-04:00	EDT	15	minute	0.004	E	kWh	delivered	false	false	usage_delivered	false	2023-04-05T03:01:02.000-04:00
102976525	102976525	2023-04-04T00:45:00.000-04:00	EDT	15	minute	0.004	E	kWh	delivered	false	false	usage_delivered	false	2023-04-05T03:01:02.000-04:00
102976525	102976525	2023-04-04T01:00:00.000-04:00	EDT	15	minute	0.005	E	kWh	delivered	false	false	usage_delivered	false	2023-04-05T03:01:02.000-04:00
