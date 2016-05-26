import pandas as pd
from datetime import datetime, date, timedelta as td
import datetime
import csv
import ConfigParser

def get_high_volume_upc(outflow, highVolumeThreshold):
    group_upc = outflow.groupby(['UPC', 'Date'])[['Qty']]
    sum_qty_upc = group_upc.sum()
    sum_qty_upc.columns = ['SumQtyUPC']
    return sum_qty_upc.loc[sum_qty_upc['SumQtyUPC'] > highVolumeThreshold]

if __name__ == '__main__':
    config = ConfigParser.ConfigParser()
    config.read("global_config.ini")
    root_data_path = config.get("Files", "root_data_path")

    config.read("get_highvolume_upc_by_daterange.ini")
    input_file = config.get("Files", "input_file")
    output_path = config.get("Files", "output_path")
    output_file_tag = config.get("Files", "output_file_tag")
    high_volume_threshold = int(config.get("Parameters", "high_volume_threshold"))

    input_path_file = root_data_path + input_file
    outflow = pd.read_pickle(input_path_file)

    high_volume_upc_details = get_high_volume_upc(outflow, high_volume_threshold)
    # set() gets unique values
    high_volume_upc = pd.DataFrame(sorted(set(high_volume_upc_details.index.get_level_values(0))))
    high_volume_upc.columns = ['UPC']
    print(high_volume_upc.columns)


    dt = datetime.datetime.now()
    details_output_filename = root_data_path + output_path + "HighVolumeUPC_" + output_file_tag + datetime.datetime.strftime(dt,"%Y%m%d_%H%M") + "_Details"
    details_output_filename_csv = root_data_path + output_path + "HighVolumeUPC_" + output_file_tag \
        + datetime.datetime.strftime(dt, "%Y%m%d_%H%M") + "_Details.csv"
    high_volume_upc_details.to_csv(details_output_filename_csv, quoting=csv.QUOTE_NONNUMERIC)
    high_volume_upc_details.to_pickle(details_output_filename)

    output_filename = root_data_path + output_path + "HighVolumeUPC_" + output_file_tag + datetime.datetime.strftime(dt,"%Y%m%d_%H%M")
    output_filename_csv = root_data_path + output_path + "HighVolumeUPC_" + output_file_tag + datetime.datetime.strftime(dt, "%Y%m%d_%H%M") + ".csv"
    high_volume_upc.to_csv(output_filename_csv, quoting=csv.QUOTE_NONNUMERIC)
    high_volume_upc.to_pickle(output_filename)

    metaData = "{\"run_datetime\": \"" + datetime.datetime.strftime(dt, "%Y%m%d_%H%M") + \
               "\", \"root_data_path\": \"" + root_data_path + "\", \"input_file\": \"" + input_file + "\"}";
    metadata_filename = root_data_path + output_path + "HighVolumeUPC_" + output_file_tag + datetime.datetime.strftime(dt,"%Y%m%d_%H%M") + ".json"
    metadata_file = open(metadata_filename, "w");
    metadata_file.write(metaData);
    metadata_file.close();
