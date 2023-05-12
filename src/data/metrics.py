import typing
import pandas as pd

from typing import List, Dict



def get_inactive_users(users: pd.DataFrame, usage: pd.DataFrame, user_access_map: Dict[str, List[str]]) -> typing.Dict[str, int]:

    # check if users has access to one AP at least
    users = users[users['userName'].apply(lambda x: x in user_access_map)]

    total_users = users['userName'].drop_duplicates().count()

    merged = users.merge(usage, left_on='userName', right_on='user', how='left')
    merged = merged.dropna(subset=['user'])

    active_users = merged['userName'].drop_duplicates().count()    
    inactive_users = total_users - active_users

    return {"total": total_users, "active": active_users, "inactive": inactive_users}


def get_used_tables(data_objects: pd.DataFrame, usage: pd.DataFrame) -> typing.Dict[str, int]:

    data_objects = data_objects[~data_objects['fullName'].str.startswith("EXTERNAL")]
    total_tables = data_objects[data_objects['type'] == 'table']["fullName"].drop_duplicates().count()
    merged = data_objects.merge(usage, left_on='fullName', right_on='dataObject', how='left')
    active_tables = merged.dropna(subset=["dataObject"])["fullName"].drop_duplicates().count()    

    return {"total": total_tables, "active": active_tables, "inactive": total_tables - active_tables}


def get_unused_access_providers(access, usage) -> typing.Dict[str, int]:
    pass

def get_queries_per_datasource(data_objects: pd.DataFrame, usage: pd.DataFrame, parent_all_children_map: Dict[str, List[str]]):

    data_sources = list(data_objects[data_objects['type'] == 'datasource']['externalId'].drop_duplicates())

    result = dict()
    for data_source in data_sources:
        print(data_source)
        usage_by_ds = usage[usage['dataObject'].apply(lambda x: x in parent_all_children_map[data_source])]
        result[data_source] = len(usage_by_ds)

    return result

def get_queries_per_user(users: pd.DataFrame, usage: pd.DataFrame, user_access_map: Dict[str, List[str]]) -> pd.DataFrame:

    users = users[users['userName'].apply(lambda x: x in user_access_map)]
    merged = users.merge(usage, left_on='userName', right_on='user', how='left')
    merged['num_queries'] = 1
    merged = merged.dropna(subset=['user'])

    q_per_user = merged[['userName', 'num_queries']].groupby('userName').sum()

    # print(q_per_user.head(4))
    return q_per_user.sort_values(by='num_queries', ascending=False).head(10)


def is_do_in_ap(data_object, ap_data_objects, parent_to_children_map) -> bool:
    result = False
    for do in ap_data_objects:
        if data_object in parent_to_children_map.get(do, []):
            return True

    return result


def get_access_provider_usage(access: pd.DataFrame, usage: pd.DataFrame, ap_to_do_map, parent_to_child_map) -> pd.DataFrame:

    aps = list(access['externalId'].drop_duplicates())
    result = dict()

    # TODO, check logic, this isn't quite right
    for ap in aps:
        usage_by_ap = usage[usage['dataObject'].apply(lambda x: is_do_in_ap(x, ap_to_do_map[ap], parent_to_child_map))]
        result[ap] = len(usage_by_ap)

    # print(access.drop(columns=['name', 'namingHint']).head(10))
    return result

def get_table_usage(data_objects: pd.DataFrame, usage: pd.DataFrame) -> pd.DataFrame:

    do = data_objects[data_objects['type'] == 'table']

    merged = do.merge(usage, left_on='fullName', right_on='dataObject', how='left')
    merged = merged.dropna(subset=['dataObject'])
    merged['num_queries'] = 1

    q_per_table = merged[['name', 'num_queries', 'startTime']].groupby('name').agg({'num_queries': 'sum', 'startTime': 'max'})

    return q_per_table.sort_values(by='num_queries', ascending=False).head(10)

def get_coverage_heatmap():
    pass

def get_usage_heatmap():
    pass