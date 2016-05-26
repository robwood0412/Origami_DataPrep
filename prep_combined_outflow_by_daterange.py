#!/usr/bin/python
from datetime import datetime, date, timedelta as td
import datetime
import os
import fnmatch
import pandas as pd
import ConfigParser


def get_paths(path, match):
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


def create_pandas(path):
    return pd.read_csv(path, names=["Date","Time","Term Id","Cashier","Seq #",
                                       "Key Function","UPC","Dept #","mlt","Price",
                                       "Qty","Wgt","Ext Price","Amt Tendered","Itemizers",
                                       "EFT #","Category","sub","sub dept","retail price type",
                                       "Freq Shopper disc","PremDisc","memberID", "SalesTot","Tax Tot",
                                       "Tran Total","Total Disc","Total Points","CashBack"])

if __name__ == '__main__':
    config = ConfigParser.ConfigParser()
    config.read("global_config.ini")
    root_data_path = config.get("Files", "root_data_path")

    config.read("prep_combined_outflow_by_daterange.ini")
    input_path = config.get("Files", "input_path")
    output_path = config.get("Files", "output_path")
    output_file_name = config.get("Files", "output_file")
    input_path_style1 = root_data_path + input_path + "/style1"
    input_path_style2 = root_data_path + input_path + "/style2"

    print(input_path_style1)
    print(input_path_style2)


    start_date = datetime.datetime.strptime(config.get("Parameters", "range_start_date"), "%Y%m%d").date()
    end_date = datetime.datetime.strptime(config.get("Parameters", "range_end_date"), "%Y%m%d").date()


    print "start_date: %s" % start_date
    print "end_date: %s" % end_date

    print(date_to_string_style1(start_date))
    print(date_to_string_style2(end_date))

    delta = end_date - start_date

    if delta.days < 0:
        print "The end_date came before the start date"
        exit(1)

    combined_outflows = pd.DataFrame()
    files_to_parse = []
    #Iterate through all the days in between the start and end day and get the paths of all the tlogs in the date range.
    for i in range(delta.days + 1):
        files_that_day = get_paths(input_path_style1, "*" + date_to_string_style1(start_date + td(days=i)))
        #TODO: Warn if len(files_that_day) > 1
        if files_that_day and len(files_that_day) > 0:
            files_to_parse.extend(files_that_day)
            combined_outflows = combined_outflows.append(create_pandas(files_that_day[0]), ignore_index=True)
            print "processed: %s" % files_that_day[0]

    for i in range(delta.days + 1):
        files_that_day = get_paths(input_path_style2, "*LOG" + date_to_string_style2(start_date + td(days=i)))
        #TODO: Warn if len(files_that_day) > 1
        if files_that_day and len(files_that_day) > 0:
            files_to_parse.extend(files_that_day)
            combined_outflows = combined_outflows.append(create_pandas(files_that_day[0]), ignore_index=True)
            print "processed: %s" % files_that_day[0]

    # TODO: Move Date Conversion from prep_analysis_data_by_daterange to here
    #combined_outflows.rename(columns={'Date': 'DateAsString'}, inplace=True)
    #combined_outflows['Date'] = [datetime.datetime.strptime(date, "%m/%d/%Y") for date in combined_outflows.DateAsString]

    print "tlog_panda.length: %s " % len(combined_outflows.index)
    files_to_parse = set(files_to_parse)

	# TODO: Add check for len(tlog_panda.index) == 0
    combined_outflows = combined_outflows[combined_outflows['Key Function'] == "UPC"]
    dt = datetime.datetime.now()
    pickle_file_name = root_data_path + output_path + output_file_name + "_" + datetime.datetime.strftime(dt, "%Y%m%d_%H%M")
    combined_outflows.to_pickle(pickle_file_name)

    metaData = "{\"run_datetime\": \"" + datetime.datetime.strftime(dt, "%Y%m%d_%H%M") + \
               "\", \"range_start_date\": \"" + date_to_string_style1(start_date) + \
               "\", \"range_end_date\": \"" + date_to_string_style1(end_date) + \
                "\", \"root_data_path\": \"" + root_data_path + \
               "\", \"input_path\": \"" + input_path + "\"}";
    metadata_filename = root_data_path + output_path + output_file_name + "_" + datetime.datetime.strftime(dt, "%Y%m%d_%H%M") + ".json"
    metadata_file = open(metadata_filename, "w");
    metadata_file.write(metaData);
    metadata_file.close();

    
    print "Searched the %s days between %s and %s and found %s" % (delta.days, date_to_string_style1(start_date), date_to_string_style1(end_date), files_to_parse)

