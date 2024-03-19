from sfi import Data, ValueLabel
from pathlib import Path
import yaml

def _add_field(varname, label, vtype, schema, value_labels):

    ftype = None
    enum = {}
    missing_values = []
    if varname in value_labels['field_to_value_label']:
        ftype = 'string'
        vlname = value_labels['field_to_value_label'][varname]
        for k, v in value_labels['value_labels'][vlname].items():
            if int(k) > 75:
                missing_values.append(v)
            else:
                enum[k] = v
    elif vtype in ['byte','int','long']:
        ftype = 'integer'
    elif vtype in ['float','double']:
        ftype = 'number'

    field = {'name':varname}
    if label:
        pass
        field['title'] = label
    if ftype:
        field['type'] = ftype
    if enum:
        field['constraints'] = {}
        field['constraints']['enum'] = enum

    schema['fields'].append(field)

    return missing_values

def _prune_duplicates(value_labels):

    map = {}
    seen = []
    names = []
    for k, v in value_labels['field_to_value_label'].items():
        if value_labels['value_labels'][v] in seen:
            map[k] = names[seen.index(value_labels['value_labels'][v])]
        else:
            map[k] = v
            seen.append(value_labels['value_labels'][v])
            names.append(v)

    return {'value_labels':dict(zip(names, seen)),
            'field_to_value_label':map}

def write_schema(schema_path, labels_path):

    schema = {}
    missing_values = []
    value_labels = {}

    value_labels['value_labels'] = {}
    for label in ValueLabel.getNames():
        values = ValueLabel.getValues(label)
        labels = ValueLabel.getLabels(label)
        vlab = {values[i]: labels[i] for i in range(len(values))}
        value_labels['value_labels'][label] = vlab

    schema['fields'] = []
    value_labels['field_to_value_label'] = {}
    nvars = Data.getVarCount()
    for v in range(nvars):
        varname = Data.getVarName(v)
        label = Data.getVarLabel(varname)
        vtype = Data.getVarType(varname)
        vlabel = ValueLabel.getVarValueLabel(varname)
        if vlabel:
            value_labels['field_to_value_label'][varname] = vlabel
        missing_values = (missing_values +
                          _add_field(varname, label, vtype, schema, value_labels)
                         )

    schema['missingValues'] = list(dict.fromkeys(missing_values))

    with open(schema_path, 'w') as f:
        yaml.dump(schema, f, sort_keys=False)

    if value_labels['value_labels']:
        value_labels = _prune_duplicates(value_labels)
        with open(labels_path, 'w') as f:
            yaml.dump(value_labels, f, sort_keys=False)
