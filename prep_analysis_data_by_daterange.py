import pandas as pd
from datetime import datetime, date, timedelta as td
import datetime
import ConfigParser
import csv

def transform_enrich_features(outflows):
    # TODO: Move Date Conversion to  prep_combined_outflow_by_daterange to here
    outflows.rename(columns={'Date': 'DateAsString'}, inplace=True)
    outflows['Date'] = [datetime.datetime.strptime(date, "%m/%d/%Y") for date in outflows.DateAsString]
    outflows['DayOfWeek'] = [datetime.datetime.weekday(date) for date in outflows.Date]
    return outflows

def get_filtered_data(combined_outflows, include_upc):

    print(len(combined_outflows))
    print(type(include_upc))
    print(include_upc.columns)
    # Filter by UPC
    filtered_outflows = combined_outflows[combined_outflows['UPC'].isin(include_upc.UPC)]
    print(filtered_outflows.columns)

    # Select Columns
    filtered_outflows = filtered_outflows[['UPC','Date','Time','Qty']]
    return filtered_outflows


if __name__ == '__main__':
    config = ConfigParser.ConfigParser()
    config.read("global_config.ini")
    root_data_path = config.get("Files", "root_data_path")

    config.read("prep_analysis_data_by_daterange.ini")
    combined_outflows_file = config.get("Files", "combined_outflows_file")
    upc_file = config.get("Files", "upc_file")
    output_path = config.get("Files", "output_path")
    output_file_tag = config.get("Files", "output_file_tag")

    upc_path_file = root_data_path + upc_file
    combined_outflows_path_file = root_data_path + combined_outflows_file

    combined_outflows = pd.read_pickle(combined_outflows_path_file)
    include_upc = pd.read_pickle(upc_path_file)

    filtered_outflows = get_filtered_data(combined_outflows, include_upc)

    enriched_outflows = transform_enrich_features(filtered_outflows)
    print(enriched_outflows.columns)

    # bucketed_outflows = bucket_outflows_by_hour(filtered_outflows)

    #print(enriched_outflows.head(n=5))
    dt = datetime.datetime.now()
    output_path_file = root_data_path + output_path + "AnalysisData_" + output_file_tag + datetime.datetime.strftime(dt, "%Y%m%d_%H%M")
    output_path_file_csv = root_data_path + output_path + "AnalysisData_" + output_file_tag + datetime.datetime.strftime(dt, "%Y%m%d_%H%M") + ".csv"

    #enriched_outflows.to_csv(output_path_file_csv, quoting=csv.QUOTE_NONNUMERIC)
    enriched_outflows.to_pickle(output_path_file)

    #TODO: Create, save metadata file

