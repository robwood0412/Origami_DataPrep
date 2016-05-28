import pandas as pd
import ConfigParser
import csv
from datetime import datetime, date, timedelta as td
import datetime

def get_all_filtered_upc(filtered_upc_filenames_filename, input_file_path):
    with open(filtered_upc_filenames_filename, 'rb') as f:
        reader = csv.reader(f)
        filtered_upc_filenames = list(reader)
        #print(type(filtered_upc_filenames))
        all_upc = pd.DataFrame()
        for filename in filtered_upc_filenames:
            print(type(filename[0]))   # TODO: Replace hack
            print(filename[0])
            filepathname = input_file_path + "/" + filename[0]
            filtered_upc = pd.read_pickle(filepathname)
            #print(type(filtered_ups))
            #print(filtered_ups)
            all_upc = all_upc.append(filtered_upc, ignore_index=True)
    return all_upc

if __name__ == '__main__':
    config = ConfigParser.ConfigParser()
    config.read("global_config.ini")
    root_data_path = config.get("Files", "root_data_path")

    config.read("get_all_highvolume_upc.ini")
    highvolume_upc_filenames_file = config.get("Files", "highvolume_upc_filenames_file")
    input_path = config.get("Files", "input_path")
    output_path = config.get("Files", "output_path")

    #TODO: Impl Report showing: UPC values that are in all files, UPC values only in file 1, 2, 1&2, etc.  Summarize with just the number of UPC values in each group

    #TODO: get from ini: /Users/robertwood/Data_project/Origami/StructuredData
    full_input_path = root_data_path + input_path
    all_upc_df = get_all_filtered_upc(highvolume_upc_filenames_file, full_input_path)
    print(len(all_upc_df.UPC))
    all_unique_upc = pd.DataFrame(list(set(all_upc_df.UPC)))  # Use set() to get unique list
    all_unique_upc.columns = ['UPC']
    print(type(all_unique_upc))
    print(all_unique_upc.columns)
    print(len(all_unique_upc.UPC))

    dt = datetime.datetime.now()
    output_filename = root_data_path + output_path + "/highvolume_upc_" + datetime.datetime.strftime(dt,"%Y%m%d_%H%M")
    all_unique_upc.to_pickle(output_filename)
    output_csv_filename = root_data_path + output_path + "/highvolume_upc_" + datetime.datetime.strftime(dt,"%Y%m%d_%H%M") + ".csv"
    all_unique_upc.to_csv(output_csv_filename)

    # TODO: Add metadata file

