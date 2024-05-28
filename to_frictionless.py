import os
import pyreadstat
import yaml


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


# Define function to write schema and value labels
def write_schema(df, meta, schema_path, labels_path):
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
        label = meta.column_names_to_labels.get(varname, "")
        vtype = df[varname].dtype.name
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
                if k in _missing_values:
                    missing_values.append(v)
                else:
                    enum.append(v)
        elif vtype in ["int64", "int32"]:
            ftype = "integer"
        elif vtype in ["float64", "float32"]:
            ftype = "number"

        # Create the field entry for the schema
        field = {"name": varname, "title": label, "type": ftype}
        if enum:
            field["constraints"] = {"enum": enum}

        schema["fields"].append(field)

    # Add missing values to the schema
    schema["missingValues"] = list(dict.fromkeys(missing_values))

    # Write schema and labels to file

    with open(schema_path, "w") as f:
        yaml.dump(schema, f, sort_keys=False)

    if value_labels["value_labels"]:
        value_labels = _prune_duplicates(value_labels)
        with open(labels_path, "w") as f:
            yaml.dump(value_labels, f, sort_keys=False)


# Main function to read SPSS file and write CSV and schema
def to_frictionless(file_path):
    filename = os.path.splitext(os.path.basename(file_path))[0]
    output_dir = f"tmp/{filename}"
    os.makedirs(output_dir, exist_ok=True)

    # Read SPSS file
    df, meta = pyreadstat.read_sav(
        file_path, apply_value_formats=True, user_missing=True
    )

    # Convert to lkower case names
    df.columns = [col.lower() for col in df.columns]
    meta.column_names = [col.lower() for col in meta.column_names]
    for attr_name in [
        "missing_ranges",
        "missing_user_values",
        "original_variable_types",
        "readstat_variable_types",
        "variable_value_labels",
        "variable_to_label",
        "column_names_to_labels"
    ]:
        metaprop = getattr(meta, attr_name)
        new_metaprop = {col.lower(): val for col, val in metaprop.items()}
        setattr(meta, attr_name, new_metaprop)    # Write the DataFrame to CSV
    csv_path = os.path.join(output_dir, f"{filename}.csv")
    df.to_csv(csv_path, index=False)

    # Write the schema and value labels to YAML
    schema_path = os.path.join("metadata","schemas", f"{filename}-schema.yaml")
    labels_path = os.path.join("metadata","value_labels","spss", f"{filename}-labels.yaml")
    write_schema(df, meta, schema_path, labels_path)
