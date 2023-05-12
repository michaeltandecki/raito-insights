import pandas as pd

from typing import Dict, List, Tuple


user_cols = ["externalId", "name", "userName", "email", "groupExternalIds"]
do_cols = ["externalId", "name", "fullName", "type", "parentExternalId"]
usage_cols = ["externalId", "accessedDataObjects", "user", "role", "startTime"]
# "accessedDataObjects":[{"dataObject":{"fullName":"MASTER_DATA.SALES.SALESORDERDETAIL","type":"table"},"permissions":["SELECT"]}]
access_cols = ["externalId","name","namingHint","access", "action", "who"]

# take the original json files and convert them to parquet files
def do_initial_processing(raw_file_dir: str, processed_file_dir: str):

    user_file = "local-raito-io-cli-plugin-snowflake-is-user-2023-02-06T13-44-51.954244+01-00-499785.json"
    data_object_file = "local-raito-io-cli-plugin-snowflake-ds-2023-02-06T13-44-05.162005+01-00-9179086.json"
    usage_file = "local-raito-io-cli-plugin-snowflake-du-2023-02-06T13-45-47.503308+01-00-4214339.json"
    access_file = "local-raito-io-cli-plugin-snowflake-da-2023-02-06T13-44-54.570996+01-00-862874.json"

    users = pd.read_json(f"{raw_file_dir}/{user_file}")
    users = users[user_cols]
    # print(users.head(2))
    users.to_parquet(f"{processed_file_dir}/users.parquet")

    data_objects = pd.read_json(f"{raw_file_dir}/{data_object_file}")
    data_objects = data_objects[do_cols]
    # print(data_objects.head(15))
    data_objects.to_parquet(f"{processed_file_dir}/data_objects.parquet")

    usage = pd.read_json(f"{raw_file_dir}/{usage_file}")
    usage = usage[usage_cols]
    usage.dropna(subset=["accessedDataObjects"], inplace=True)
    usage["dataObject"] = usage["accessedDataObjects"].apply(lambda x: x[0].get('dataObject', '').get('fullName', ''))
    usage["doType"] = usage["accessedDataObjects"].apply(lambda x: x[0].get('dataObject', '').get('type', ''))
    usage["action"] = usage["accessedDataObjects"].apply(lambda x: x[0].get('permissions', [""])[0])
    usage = usage.drop(columns=["accessedDataObjects"])
    usage.to_parquet(f"{processed_file_dir}/data_usage.parquet")


    # data examples
    # "who":{"users":["BART","DATA_ENGINEERING","RAITO"],"groups":[],"accessProviders":[]}
    # [{"actualName":"SALES_EXT","what":[{"dataObject":{"fullName":"MASTER_DATA.PERSON.ADDRESS","type":"TABLE"},"permissions":["SELECT"]}, ... ,{"dataObject":{"fullName":"MASTER_DATA.SALES.STORE","type":"TABLE"},"permissions":["SELECT"]}]}]

    access = pd.read_json(f"{raw_file_dir}/{access_file}")
    access = access[access_cols]
    access["users"] = access["who"].apply(lambda x: x.get('users', []) if x is not None else [])
    access["groups"] = access["who"].apply(lambda x: x.get('groups', []) if x is not None else [])
    access["accessProviders"] = access["who"].apply(lambda x: x.get('accessProviders', []) if x is not None else [])
    # print(access.dropna().head(2))
    access.to_parquet(f"{processed_file_dir}/access.parquet")

    return users, data_objects, usage, access


def is_resolved(access_map):
    resolved = True
    for k, v in access_map.items():
        if len(v.get('aps', [])) > 0:
            return False
    return resolved


def unpack_users(access_map: Dict[str, Dict[str, List[str]]]):
    
    while not is_resolved(access_map):        
        for k, v in access_map.items():
            if len(v.get('aps', [])) == 0:
                continue
                            
            for ap in v.get('aps', []):
                if len(access_map[ap].get('aps', [])) > 0:
                    continue
                v['users'] += access_map[ap].get('users', [])
                v['aps'].remove(ap)
        
    for k, v in access_map.items():
        access_map[k]['users'] = list(set(access_map[k]['users']))
    
    
    return access_map

def get_access_maps(access: pd.DataFrame) -> Tuple[Dict[str, Dict[str, List[str]]], Dict[str, List[str]]]:
    access_map = dict()
    for _, row in access.iterrows():
        if row['action'] == 'Grant':
            access_map[row['name']] = {'users': list(row['users']), 'aps': list(row['accessProviders'])}
    
    access_map = unpack_users(access_map)


    user_access_map = dict()
    for k, v in access_map.items():
        for user in v.get('users', []):
            if not user in user_access_map:
                user_access_map[user] = []
            user_access_map[user].append(k)

    return access_map, user_access_map


def get_ap_to_do_map(access: pd.DataFrame) -> Dict[str, List[str]]:
    ap_to_do_map = dict()
    for _, row in access.iterrows():
        a = row['access']
        dos = []
        for access_element in a:
            what = access_element.get('what')
            for w in what:
                item = w.get('dataObject', {}).get('fullName')
                if item:
                    dos.append(item)
        ap_to_do_map[row['name']] = list(set(dos))

    return ap_to_do_map


def get_do_maps(data_objects: pd.DataFrame):

    child_parent_map = dict()
    # TODO, check what the externalId/name for the data source means
    for _, row in data_objects.iterrows():
        child_parent_map[row['externalId']] = row['parentExternalId']

    child_to_all_parents_map = dict()
    for k, v in child_parent_map.items():
        ancestors = []
        parent = child_parent_map[k]
        while parent != "":
            ancestors.append(parent)        
            parent = child_parent_map[parent]
        child_to_all_parents_map[k] = ancestors
        
    parent_to_all_children_map = dict()
    for k, v in child_to_all_parents_map.items():
        if len(v) == 0:
            continue
            
        for parent in v:
            if not parent in parent_to_all_children_map:
                parent_to_all_children_map[parent] = []
            parent_to_all_children_map[parent].append(k)   

    return child_parent_map, child_to_all_parents_map, parent_to_all_children_map