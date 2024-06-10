from to_frictionless import to_frictionless
from config import DATASETS
from pathlib import Path
import json

descriptionmap = json.loads(Path("descriptionmap.json").read_text())
titlemap = json.loads(Path("titlemap.json").read_text())
renamemap = json.loads(Path("renamemap.json").read_text())
excludemap = json.loads(Path("exclude_from_release_map.json").read_text())

def select_filemapping(map,filename):

    new_mapping = {}
    for colname,item in map.items():
        
        if isinstance(item,str):
            new_mapping[colname] = item
        elif item.get(filename):
            new_mapping[colname] = item[filename]

    return new_mapping

for dataset in DATASETS:

    filename = dataset["filename"]

    file_descriptionmap = select_filemapping(descriptionmap,filename)
    file_titlemap = select_filemapping(titlemap,filename)
    file_renamemap = select_filemapping(renamemap,filename)

    filename += ".sav"
    basepath = "data/protocol1-omnibus/"
    to_frictionless(basepath+filename,
        renamemap=file_renamemap,
        descriptionmap=file_descriptionmap,
        titlemap=file_titlemap,
        excludemap=excludemap)