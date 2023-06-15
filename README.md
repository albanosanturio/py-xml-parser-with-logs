# py-xml-parser-with-logs

#### Priorities:
1.Add a sort by OPCO** functionality. (Need a SAP extract)
2.Only process ids included in a "approved meters id" list (Need that file format)
3.Removing unnecesary meters from being processed (ids starting with "600")
4.Find any other efficiencies

#### Extra:
Repo
Config file
Requirements

#### Follow up questions:
Is info from "SAP Extract by opco" and "Approved meters id" redundant?
Should we filter by both?
Both previous info... Format? Size? Frequency? Will it get to be a huge file over time? Does it accumulate?
Difference in NY and CMP codes?
Any other difference that comes with opco?
Ask Steve adams for sample info about: Approved meters_id && SAP extract by OPCO

#### Currently WIP:
Setting environment locally and making the test runs work same as bryan



#### Sample XML input

 <Channels>
        <Channel IsRegister="false" MarketType="Electric" IntervalLength="15" NumberOfDials="-1" PulseMultiplier="-1" PressureCompensationFactor="-1" IsReadingDecoded="true" ReadingsInPulse="false">
        <ChannelID ServicePointChannelID="6001118806:1"/>
            <ContiguousIntervalSets>
                <ContiguousIntervalSet NumberOfReadings="96">
                <TimePeriod StartTime="2022-12-05T05:00:00Z" EndTime="2022-12-06T05:00:00Z"/>
                    <Readings>
                        <Reading Value="0" StatusRef="62"/>
                        <Reading Value="0" StatusRef="62"/>
                        <Reading Value="0" StatusRef="62"/>
                        <Reading Value="0.151" StatusRef="59"/>
                    </Readings>
                </ContiguousIntervalSet>
            </ContiguousIntervalSets>
        </Channel>
        <Channel IsRegister="true" MarketType="Electric" IntervalLength="0" NumberOfDials="-1" PulseMultiplier="-1" PressureCompensationFactor="-1" IsReadingDecoded="true" ReadingsInPulse="false">
        <ChannelID ServicePointChannelID="6001118806:101"/>
            <Readings>
                <Reading Value="941.787" ReadingTime="2022-12-06T05:00:00Z" StatusRef="60"/>
            </Readings>
        </Channel>
 </Channels>