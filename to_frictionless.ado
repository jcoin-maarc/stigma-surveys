// Translate SPSS file into CSV format, with schema and value labels

prog def to_frictionless

    version 18.0
    syntax using/ [, case(passthru) clear replace]

    mata: st_local("filename", pathrmsuffix(pathbasename(`"`using'"')))
    if `"`case'"' == "" {
        loc case "case(lower)"
    }

    import spss `using', `case' `clear'

    foreach var of varlist _all {
        if substr("`:format `var''",1,3) == "%td" {
            format `var' %tdCCYY-NN-DD
        }
        else if substr("`:format `var''",1,3) == "%tc" {
            format `var' %tcCCYY-NN-DD!THH:MM:SS
        }
    }

    cap mkdir "tmp"
    cap mkdir "tmp/`filename'"
    export delim using "tmp/`filename'/`filename'.csv", delim(",") `replace'

    python: write_schema("""tmp/`filename'/`filename'-schema.yaml""",  ///
                         """tmp/`filename'/`filename'-labels.yaml""")

end

python:

from sfi import Data, ValueLabel
import yaml

def _add_field(varname, label, vtype, vformat, schema, value_labels):

    ftype = None
    enum = []
    missing_values = []
    if vformat[0:3] == "%td":
        ftype = 'date'
    elif vformat[0:3] == "%tc":
        ftype = 'datetime'
    elif varname in value_labels['field_to_value_label']:
        ftype = 'string'
        vlname = value_labels['field_to_value_label'][varname]
        for k, v in value_labels['value_labels'][vlname].items():
            if int(k) > 75:
                missing_values.append(v)
            else:
                enum.append(v)
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
        vformat = Data.getVarFormat(varname)
        vlabel = ValueLabel.getVarValueLabel(varname)
        if vlabel:
            value_labels['field_to_value_label'][varname] = vlabel
        missing_values = (missing_values +
                          _add_field(varname, label, vtype, vformat, schema,
                                     value_labels)
                         )

    schema['missingValues'] = list(dict.fromkeys(missing_values))

    with open(schema_path, 'w') as f:
        yaml.dump(schema, f, sort_keys=False)

    if value_labels['value_labels']:
        value_labels = _prune_duplicates(value_labels)
        with open(labels_path, 'w') as f:
            yaml.dump(value_labels, f, sort_keys=False)

end
