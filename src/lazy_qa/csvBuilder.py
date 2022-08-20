import json
import csv
import pandas as pd
import os


# ---------------------------------------------------------------------------------------------------------------------#
#                                       Functions to manipulate CSV data files                                         #
# ---------------------------------------------------------------------------------------------------------------------#

# -------------------------------------------------- CSV Functions ----------------------------------------------------#


def delete_output_file(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)


def load_csv(csv_file_path):
    """
    Load all lines from a csv.
    Given this csv
        account_id    sku
        80589819      BBDREN0330024M
        #$%%          BCOR!!!!!!!!!!

    If valid data is selected in test_scenario_id, the result will be:
        [
            {'account_id': '80589819', 'sku': 'BBDREN0330024M'}, {'account_id': '#$%%', 'sku': 'BCOR!!!!!!!!!!'}
        ]

     The yaml configuration file should be like this:
                csv_strategy: all_in
                csv_data_source: Some_csv_data_source
    :param csv_file_path: csv file with data sample
    :return:
    """
    new_json = []
    with open(csv_file_path, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            new_json.append(json.dumps(row, sort_keys=True))
    return new_json


def load_csv_multiple_lines(csv_file, group_key, output_list_name, list_fields):
    """
    Load a configuration from a csv in multiple lines and group it in a single line.
    Given this csv
        deliveryCenterId	vendorItemId	quantity
        DC001	            SKU001	        10
        DC001	            SKU002	        20
        DC002	            SKU005	        50
    The result will be:
        [
            {'deliveryCenterId': 'DC001', 'inventory': [{'vendorItemId': SKU001, 'quantity': 10},
                                        {'vendorItemId': SKU002, 'quantity': 20}]},
            {'deliveryCenterId': 'DC002', 'inventory': [{'vendorItemId': SKU005, 'quantity': 10}]}
        ]

     The yaml configuration file should be like this:
                csv_strategy: multiple_lines
                multiple_request: true
                multiple_line_config:
                  group_key:
                    - deliveryCenterId
                  output_list_name: inventory
                  list_fields:
                    - vendorItemId
                    - quantity
    :param csv_file: csv file with data sample
    :param group_key: list of fields to group the data
    :param output_list_name: output column name
    :param list_fields: fields to be grouped in the list of `output_list_name`
    :return:
    """
    result = {}
    with open(csv_file, 'r') as fh:
        csv_reader = csv.DictReader(fh)
        for row in csv_reader:
            group_key_joined = get_group_key(row, group_key)
            if group_key_joined not in result:
                result[group_key_joined] = row.copy()
                result[group_key_joined][output_list_name] = []
            result[group_key_joined][output_list_name].append({field: row[field] for field in list_fields})

    return [json.dumps(data) for data in result.values()]


def get_group_key(row, group_key):
    return "_".join(str(row[r]) for r in row if r in group_key)


def get_scenario_data_csv(csv_file_path, test_scenario_id):
    """
    Load a data scenario from a csv in a single line.
    Given this csv
        test_scenario_id   account_id    sku
        valid data         80589819      BBDREN0330024M
        invalid data       #$%%          BCOR!!!!!!!!!!

    If valid data is selected in test_scenario_id, the result will be:
        [
            {'account_id': '80589819', 'sku': 'BBDREN0330024M'}
        ]

     The yaml configuration file should be like this:
                csv_strategy: simple_line
                csv_data_source: Some_csv_data_source
                csv_scenario: 'valid data, invalid data, empty data'
    :param csv_file_path: csv file with data sample
    :return:
    """
    new_json = []
    with open(csv_file_path, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            if test_scenario_id == row["test_scenario_id"]:
                new_json.append(json.dumps(row, sort_keys=True))
    return new_json


def get_each_line_data_csv(csv_file_path):
    """
    Load a data scenario from a csv in a single line.
    Given this csv
        test_scenario_id   account_id    sku
        valid data         80589819      BBDREN0330024M
        invalid data       #$%%          BCOR!!!!!!!!!!

    If valid data is selected in test_scenario_id, the result will be:
        [
            {'account_id': '80589819', 'sku': 'BBDREN0330024M'}
        ]

     The yaml configuration file should be like this:
                csv_strategy: simple_line
                csv_data_source: Some_csv_data_source
                csv_scenario: 'valid data, invalid data, empty data'
    :param csv_file_path: csv file with data sample
    :return:
    """
    new_json = []
    with open(csv_file_path, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            new_json.append(json.dumps(row, sort_keys=True))
    return new_json


def converter_pandas_csv_json(data_path):
    df = pd.read_csv(data_path)
    new_json = df.to_json(orient='records')
    return new_json
