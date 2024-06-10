
""" 
takes an excel worksheet and adds modified hardcoded information 

NOTE: if using xlsx need openpyxl

#TODO: put check on duplicate target names (e.g., rename.values() duplicates)
""" 

import pandas as pd 
import frictionless
from config import DATASETS
import json

def parse_prop(prop):

    if pd.isna(prop) or not prop:
        return None
    else:
        parsedprop = [
            tuple(_v.strip() for _v in v.split("="))
            for v in prop.split("|")
        ]
        
        if len(parsedprop[0]) > 1:
            return dict(parsedprop)
        else:
            return prop


crosswalk = pd.read_excel("protocol1_data_dictionary_v4.xlsx")
crosswalk.set_index("name",inplace=True)

renamemap = {}
descriptionmap = {}
titlemap = {}
excludemap = {}

for name,metadata in crosswalk.T.items():

    # rename all that have renamings
    if pd.notna(metadata["custom.renameTo"]) and metadata["custom.renameTo"]:
        renamemap[name] = parse_prop(metadata["custom.renameTo"])

    # rename all that have renamings
    if pd.notna(metadata["exclude_from_release"]) and metadata["exclude_from_release"]:
        excludemap[name] = parse_prop(metadata["exclude_from_release"])
   
   # the field names below have different annotations depending on the renamed survey
    parse_titles = [
        "q25",
        "q25a",
    ]
    parse_descriptions = parse_titles + ["kb_rockbottom"]
    
    # parse descriptions
    if name in parse_descriptions:
        descriptionmap[name] = parse_prop(metadata["description"])
    elif pd.notna(metadata["description"]) and metadata["description"]:
        descriptionmap[name] = metadata["description"]

    # parse titles
    if name in parse_titles:
        titlemap[name] = parse_prop(metadata["title"])
    elif pd.notna(metadata["title"]) and metadata["title"]:
        titlemap[name] = metadata["title"]

num_to_name = {d["name"].replace("survey",""):d["filename"] for d in DATASETS}
rename_num_to_name = lambda r: {num_to_name[k]:r[k] for k in r} if isinstance(r,dict) else r
pd.Series(renamemap).apply(rename_num_to_name)

pd.Series(renamemap).apply(rename_num_to_name).to_json("renamemap.json",indent=2)
pd.Series(descriptionmap).apply(rename_num_to_name).to_json("descriptionmap.json",indent=2)
pd.Series(titlemap).apply(rename_num_to_name).to_json("titlemap.json",indent=2)
pd.Series(excludemap).apply(lambda v: [num_to_name[str(_v)] for _v in json.loads(v)]).to_json("exclude_from_release_map.json")