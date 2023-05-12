import os
import json

import pandas as pd

from data.metrics import *
from data.processing import *
from data.utils import NumpyEncoder


if __name__ == "__main__":

    print(f"Working directory {os.getcwd()}")

    raw_file_dir = "data/raw"
    clean_file_dir = "data/clean"

    users, data_objects, usage, access = do_initial_processing(raw_file_dir, clean_file_dir)

    access_user_map, user_access_map = get_access_maps(access)

    child_parent_map, child_to_all_parents_map, parent_to_all_children_map = get_do_maps(data_objects)

    ap_to_do_map = get_ap_to_do_map(access)

    result = dict()

    active_users = get_inactive_users(users, usage, user_access_map)
    result["active_users"] = active_users
    used_tables = get_used_tables(data_objects, usage)
    result["used_tables"] = used_tables
    used_aps = get_unused_access_providers(access, usage)
    result["used_aps"] = used_aps

    print(f"Active users: {active_users}")
    print(f"Used tables: {used_tables}")
    print(f"Unused Access providers: {used_aps}")


    ds_queries = get_queries_per_datasource(data_objects, usage, parent_to_all_children_map)
    user_queries = get_queries_per_user(users, usage, user_access_map)
    ap_usage = get_access_provider_usage(access, usage, ap_to_do_map, parent_to_all_children_map)
    table_usage = get_table_usage(data_objects, usage)

    print(f"Queries per data source: {ds_queries}")
    print(f"Queries per user: {user_queries}")
    print(f"Queries per access provider: {ap_usage}") 
    print(f"Queries per table: {table_usage} ")

    print(f"Heatmap coverage: ") #TODO
    print(f"Heatmap usage: ") #TODO



    # TODO
    # * process access providers
    # * flatten data_object file (add datasource, parent?)
    # * html template

    # create report: https://towardsdatascience.com/how-to-easily-create-a-pdf-file-with-python-in-3-steps-a70faaf5bed5

    report = ""
    with open('resources/template/report.html', 'r') as file:
        report = file.read()

    with open('resources/metrics_example.json', 'w') as file:
        json.dump(result, file, indent=2, cls=NumpyEncoder)

    for k1, v1 in result.items():
        if v1 is None:
            continue
        for k2, v2 in v1.items():
            print()
            report = report.replace("{{" + k1 + "." + k2 + "}}", str(v2))

    with open('resources/template/report_latest.html', 'w') as file:
        file.write(report)
    