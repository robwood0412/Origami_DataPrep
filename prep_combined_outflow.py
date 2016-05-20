#!/usr/bin/python
from datetime import datetime, date, timedelta as td
import datetime
import os
import fnmatch
import pandas as pd
import ConfigParser


def getPaths(path, match):
    paths = []
    for root, dirnames, filenames in os.walk(path):
        for filename in fnmatch.filter(filenames, match):
            paths.append(os.path.join(root, filename))
    return paths

def date(value):
    return datetime.datetime.strptime(value, "%Y%m%d").date()

def date_to_string_style1(date):
    return str(date.year) + str(date.month).zfill(2) + str(date.day).zfill(2) 

def date_to_string_style2(date):
    return str(date.month).zfill(2) + str(date.day).zfill(2) + str(date.year)

#def process_tlog_file(directory_path, tlog_file):
#    full_path = join(directory_path, tlog_file)

def create_pandas(path):
    return pd.read_csv(path, names=["Date","Time","Term Id","Cashier","Seq #",
                                       "Key Function","UPC","Dept #","mlt","Price",
                                       "Qty","Wgt","Ext Price","Amt Tendered","Itemizers",
                                       "EFT #","Category","sub","sub dept","retail price type",
                                       "Freq Shopper disc","PremDisc","memberID", "SalesTot","Tax Tot",
                                       "Tran Total","Total Disc","Total Points","CashBack"])

if __name__ == '__main__':
    config = ConfigParser.ConfigParser()
    config.read("prep_combined_outflow.ini")

    inputDirectory = config.get("Directories", "input_directory")
    outputDirectory = config.get("Directories", "output_directory")
    tlog_directory_style1 = inputDirectory + "/style1"
    tlog_directory_style2 = inputDirectory + "/style2"

    print(tlog_directory_style1)
    print(tlog_directory_style2)


    start_date = datetime.datetime.strptime(config.get("DateRange", "RangeStartDate"), "%Y%m%d").date()
    end_date = datetime.datetime.strptime(config.get("DateRange", "RangeEndDate"), "%Y%m%d").date()


    print "start_date: %s" % start_date
    print "end_date: %s" % end_date

    print(date_to_string_style1(start_date))
    print(date_to_string_style2(end_date))

    delta = end_date - start_date

    if delta.days < 0:
        print "The end_date came before the start date"
        exit(1)

    tlog_panda = pd.DataFrame()
    files_to_parse = []
    #Iterate through all the days in between the start and end day and get the paths of all the tlogs in the date range.
    for i in range(delta.days + 1):
        files_that_day = getPaths(tlog_directory_style1, "*" + date_to_string_style1(start_date + td(days=i)))
        if files_that_day and len(files_that_day) > 0:
            files_to_parse.extend(files_that_day)
            tlog_panda = tlog_panda.append(create_pandas(files_that_day[0]), ignore_index=True)

    for i in range(delta.days + 1):
        files_that_day = getPaths(tlog_directory_style2, "*LOG" + date_to_string_style2(start_date + td(days=i)))
        if files_that_day and len(files_that_day) > 0:
            files_to_parse.extend(files_that_day)
            tlog_panda = tlog_panda.append(create_pandas(files_that_day[0]), ignore_index=True)
            
    print "tlog_panda.length: %s " % len(tlog_panda.index)
    files_to_parse = set(files_to_parse)

	# TODO: Add check for len(tlog_panda.index) == 0
    tlog_panda = tlog_panda[tlog_panda['Key Function'] == "UPC"]
    dt = datetime.datetime.now()
    pickle_file_name = outputDirectory + "/combined_outflow_" + datetime.datetime.strftime(dt, "%Y%m%d_%H%M")
    tlog_panda.to_pickle(pickle_file_name)

    # TODO: Create metadata JSON file
    metaData = "{\"RunDateTime\": \"" + datetime.datetime.strftime(dt, "%Y%m%d_%H%M") + \
               "\", \"RangeStartDate\": \"" + date_to_string_style1(start_date) + \
               "\", \"RangeEndDate\": \"" + date_to_string_style1(end_date) + \
               "\", \"InputDirectory\": \"" + inputDirectory + "\"}";
    metaDataFileName = outputDirectory + "/meta_data.combined_outflow_" + datetime.datetime.strftime(dt, "%Y%m%d_%H%M") + ".json"
    metaDataFile = open(metaDataFileName, "w");
    metaDataFile.write(metaData);
    metaDataFile.close();

    
    print "Searched the %s days between %s and %s and found %s" % (delta.days, date_to_string_style1(start_date), date_to_string_style1(end_date), files_to_parse)

