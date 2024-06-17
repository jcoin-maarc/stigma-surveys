import os
import pyreadstat
import yaml
from pathlib import Path

import config
import cleaning

def _prune_duplicates(value_labels):

    map = {}
    seen = []
    names = []
    for k, v in value_labels["field_to_value_label"].items():
        if value_labels["value_labels"][v] in seen:
            map[k] = names[seen.index(value_labels["value_labels"][v])]
        else:
            map[k] = v
            seen.append(value_labels["value_labels"][v])
            names.append(v)

    return {"value_labels": dict(zip(names, seen)), "field_to_value_label": map}

    # Write schema and labels to file

def _write_schema(schema,filename):
    """ Outputs a schema and associated value laels to separate files """ 

    schema_path = os.path.join("metadata","schemas", f"{filename}-schema.yaml")

    with open(schema_path, "w") as f:
        yaml.dump(schema, f, sort_keys=False)


def _write_value_labels(value_labels,filename):
    """ outputs value labels  to yaml """
    labels_path = os.path.join("metadata","value_labels","spss", f"{filename}-labels.yaml")
    if value_labels["value_labels"]:
        value_labels = _prune_duplicates(value_labels)
    with open(labels_path, "w") as f:
        yaml.dump(value_labels, f, sort_keys=False)
    
    
def to_schema_and_value_labels(df, meta):
    """ Uses the output of pyreadstat to generate a schema with value labels """ 

    schema = {"fields": []}
    value_labels = {}
    value_labels["value_labels"] = {
        lblname: {int(val): lbl for val, lbl in lbls.items()}
        for lblname, lbls in meta.value_labels.items()
    }
    value_labels["field_to_value_label"] = meta.variable_to_label
    missing_values = []

    # Process each variable in the DataFrame
    for varname in df.columns:
        enum = []
        vtype = df[varname].convert_dtypes().dtype.name.lower()
        vformat = meta.original_variable_types.get(varname, "")
        # Determine the field type and other attributes
        #  NOTE: dates taken from https://libguides.library.kent.edu/SPSS/DatesTime
        ftype = None

        if vformat.startswith("DATETIME"):
            ftype = "datetime"
        elif vformat.startswith("DATE"):
            ftype = "date"
        elif varname in value_labels["field_to_value_label"]:
            ftype = "string"
            vlname = value_labels["field_to_value_label"][varname]
            _missing_values = []
            for vals in meta.missing_ranges.get(varname,[]):
                if vals["lo"] == vals["hi"]:
                    _missing_values.append(vals["lo"])
                else:
                    raise Exception("Only discrete missing values are currently supported")

            for k, v in value_labels["value_labels"][vlname].items():
                if k in _missing_values or k > 75: # some obvious missing values not marked as missing so > 75
                    missing_values.append(v)
                else:
                    enum.append(v)
        elif vtype in ["int64", "int32"]:
            ftype = "integer"
        elif vtype in ["float64", "float32"]:
            ftype = "number"
        elif vtype in ["category","string"]:
            ftype = "string"
        elif vtype == "object":
            ftype = "any"


        # Create the field entry for the schema

        field = {"name": varname}

        # get title 
        label = meta.column_names_to_labels.get(varname)
        if label: 
            field["title"] = label
        # get descriptions
        if hasattr(meta,"column_names_to_description"): 
            description = meta.column_names_to_description.get(varname)
            if description: 
                field["description"] = meta.column_names_to_description[varname]

        field["type"] = ftype

        if enum:
            field["constraints"] = {"enum": enum}
        if hasattr(meta,"column_names_to_exclude"): 
            exclude = meta.column_names_to_exclude.get(varname)
            if exclude: 
                field["custom"] = field.get("custom",{"exclude_from_release":True})
            
        schema["fields"].append(field)

    # Add missing values to the schema
    schema["missingValues"] = list(dict.fromkeys(missing_values))
    return schema,value_labels



# Main function to read SPSS file and write CSV and schema
def to_frictionless(file_path,renamemap=None,descriptionmap=None,titlemap=None,excludemap=None):
    """ 
    Convert a spss file (from file path) to 
    machine readable frictionless resources (csv), 
    schema (yaml), value_labels (yaml).

    option to override spss file metadata with additional 
    metadata or new metadata (using same naming conventions as metadata object)
    
    (e.g., meta_additional = {"variable_description":<mapping>,"column_names_to_labels})


    """
    filename = os.path.splitext(os.path.basename(file_path))[0]
    output_dir = f"data/protocol1-omnibus"
    os.makedirs(output_dir, exist_ok=True)

    # Read SPSS file
    df, meta = pyreadstat.read_sav(
        file_path,user_missing=True
    )
    df = df.convert_dtypes()
    df = pyreadstat.set_value_labels(df,meta) # set labels last separate so float --> int conversions can happen
    ## Add descriptions to user defined (non-standard!)
    if descriptionmap:
        meta.column_names_to_description = descriptionmap
    ## Change titles to user defined
    if titlemap:
        meta.column_names_to_labels = titlemap

    if excludemap:
        meta.column_names_to_exclude = excludemap

    pyreadstat_meta_vars = [
        "missing_ranges",
        "missing_user_values",
        "original_variable_types",
        "readstat_variable_types",
        "variable_value_labels",
        "variable_to_label",
        "column_names_to_labels",
        "column_names_to_description", # NOTE: this is a custom prop (not in pyreadstat)
        "column_names_to_exclude" # NOTE: this is a custom prop (not in pyreadstat)
    ]

    # lower case all pyreadstat vars and data
    df.columns = [col.lower() for col in df.columns]
    meta.column_names = [col.lower() for col in meta.column_names]
    for attr_name in pyreadstat_meta_vars:
        metaprop = getattr(meta, attr_name)
        new_metaprop = {colname.lower():metaprop[colname] for colname in list(metaprop)}
        setattr(meta, attr_name, new_metaprop)

    # add user-defined pyreadstat metadata variable replacements

    # Rename all variables based on the inputted rename map 
    df.rename(columns=renamemap,inplace=True)
    if renamemap:
        for attr_name in pyreadstat_meta_vars:
            metaprop = getattr(meta, attr_name)
            new_metaprop = {renamemap.get(colname,colname):metaprop[colname] for colname in list(metaprop)}
            setattr(meta, attr_name, new_metaprop)

    # Write the schema and value labels to YAML
    schema,value_labels = to_schema_and_value_labels(df,meta)

    survey_name = config.FILENAME_TO_NAME[Path(file_path).stem]

    if hasattr(cleaning,survey_name):
        df,schema,value_labels = getattr(cleaning,survey_name)(df,schema,value_labels)
    else:
        raise Exception(f"No cleaning function for {survey_name}!")
    
    # add default missing value (blank)
    if not "" in schema["missingValues"]:
        schema["missingValues"].append("")

    # Write data/metadata to file
    csv_path = os.path.join(output_dir,"csv",f"{filename}.csv")
    df.to_csv(csv_path, index=False)
    _write_schema(schema,filename)
    _write_value_labels(value_labels,filename)
