import pandas as pd
from datetime import datetime, date, timedelta as td
import datetime
import ConfigParser
import csv

# TODO:
#def bucket_outflows_by_hour(outflows):



if __name__ == '__main__':
    config = ConfigParser.ConfigParser()
    config.read("global_config.ini")
    root_data_path = config.get("Files", "root_data_path")

    config.read("prep_hour_bucket_by_daterange.ini")
    analysis_data_file = config.get("Files", "analysis_data_file")
    output_path = config.get("Files", "output_path")
    output_file_tag = config.get("Files", "output_file_tag")

    analysis_data_path_file = root_data_path + analysis_data_file
    analysis_data = pd.read_pickle(analysis_data_path_file)

    #bucket_outflows = bucket_outflows_by_hour(analysis_data)

    # Move to prep_hourbucket_data
    #output_filename = output_path + "HourBucketOutflows_" + output_file_tag + datetime.datetime.strftime(dt,"%Y%m%d_%H%M")
    #output_filename_csv = output_path + "HourBucketOutflows_" + output_file_tag + datetime.datetime.strftime(dt, "%Y%m%d_%H%M") + ".csv"

