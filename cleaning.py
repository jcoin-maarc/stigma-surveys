# Data and metadata cleaning
import pandas as pd
import frictionless as fl

def _replace_vals(df,schema,value_labels,to_replace):
    """ replaces missing values in data, schema, and value labels based on a `to_replace` dictionary of current:replaced keyval pairs
    
    inplace operation
    
    """
    df.replace(to_replace,inplace=True)
    schema["missingValues"] = [to_replace.get(item, item) for item in schema["missingValues"] if not to_replace.get(item)]
    value_labels["value_labels"] = pd.Series(value_labels["value_labels"]).replace(to_replace).to_dict()

    for field in schema["fields"]:
        enums = field.get("constraints", {}).get("enum", [])
        if enums:
            for i in range(len(enums)):
                if to_replace.get(enums[i]):
                    enums[i] = to_replace.get(enums[i], enums[i])

def _delete_metadata_vals(df,schema,value_labels,to_delete):
    """ 
    
    deletes missing values in data, schema, and value labels based on a list of to-be-deleted
    missing values. 
    Currently just does metadata -- if needed will add dataframe (if caught during validation at any point)
    
    inplace operation

    
    """
    schema["missingValues"] = [item for item in schema["missingValues"] if not item in to_delete]
    value_labels["value_labels"] = pd.Series(value_labels["value_labels"]).loc[lambda s: ~s.isin(to_delete)].to_dict()
    
    for field in schema["fields"]:
        enums = field.get("constraints", {}).get("enum", [])
        if enums:
            field["constraints"]["enum"] = [item for item in enums if item not in to_delete]

def _change_duration_jcoin(df,schema):

    df.replace({"duration_jcoin":{"Under 1 minute":"0"}},inplace=True)
    for field in schema["fields"]:
        if field["name"] == "duration_jcoin":
            del field["constraints"]
            field["type"] = "integer"

def survey1(df,schema,value_labels):
    """Clean Survey 1 data."""
    df = df.copy()
    schema = schema.copy()

    _delete_metadata_vals(df,schema,value_labels,["Under 18"])
    _change_duration_jcoin(df,schema)

    # return (df
    #         .assign(employ=df.employ.str.replace('–','-'))
    # )
    return df,schema,value_labels

def survey2(df,schema,value_labels):
    """Clean Survey 2 data."""
    df = df.copy()
    schema = schema.copy()

    replace_missing_vals = {"Don\u2019t know":"DON'T KNOW"}
    delete_missing_vals = ["Under 18"]

    _replace_vals(df,schema,value_labels,replace_missing_vals)
    _delete_metadata_vals(df,schema,value_labels,delete_missing_vals)
    _change_duration_jcoin(df,schema)
    # return (df
    #         .assign(employ=df.employ.str.replace('–','-'))
    #         .replace({'familyuse_ever':{'Don’t know':"DON'T KNOW"}})
    # )
    return df,schema,value_labels

def survey3(df,schema,value_labels):
    """Clean Survey 3 data."""
    df = df.copy()
    schema = schema.copy()

    _change_duration_jcoin(df,schema)
    _delete_metadata_vals(df,schema,value_labels,["Under 18"])

    # df = (df
    # #         .assign(employ=df.employ.str.replace('–','-'))
    # #         .replace({'personaluse_last':{'6 – 12 months ago':'6-12 months ago'},
    # #                   'duration_norcnd':{'Under 1 minute':'0'}})
    #  )
    return df,schema,value_labels

def survey4(df,schema,value_labels):
    """Clean Survey 4 data."""
    df = df.copy()
    schema = schema.copy()
    replace_missing_vals = {"Dont Know":"DON'T KNOW",
    "Skipped on Web":"SKIPPED ON WEB",
    "Refused":"REFUSED",
    "Unknown":"",
    "DontKnow":"DON'T KNOW"}
    # return (df
    #         .assign(employ=df.employ.str.replace('–','-'))
    #         .replace({'familyuse_ever':{'0':'No'},
    #                   'vaxplans':{'DontKnow':"DON'T KNOW"}})
    # )
    _replace_vals(df,schema,value_labels,replace_missing_vals)
    _delete_metadata_vals(df,schema,value_labels,["Under 18"])
    _change_duration_jcoin(df,schema)
    return df,schema,value_labels

def survey5(df,schema,value_labels):
    """Clean Survey 5 data."""
    df = df.copy()
    schema = schema.copy()
    df = (df
            .assign(employ=df.employ.str.replace('–','-'))
            .replace({'personalmisuse_lifetime':{'77':"DON'T KNOW",
                                                 '98':'SKIPPED ON WEB',
                                                 '99':'REFUSED'},
                      'famfriendmisuse_lifetime':{'77':"DON'T KNOW",
                                                  '98':'SKIPPED ON WEB',
                                                  '99':'REFUSED'},
                      'personalmisuse_recent':{'77':"DON'T KNOW",
                                               '98':'SKIPPED ON WEB',
                                               '99':'REFUSED'},
                      #'personaluse_last':{'7 – 12 months ago':'7-12 months ago'},
                      #'pidi':{"Don’t lean":"Don't lean"},
                      #'vaxplans':{'Not sure':'NOT SURE'}
            }
                    )
    )
    dk = "DON'T KNOW"
    replace_missing_vals = {"Don\u2019t know/unsure":dk,
        "Don\u2019t know":dk,
        "Don't know":dk,
        "DON\u2019T KNOW":dk,
        "Not sure":dk,
        "I don't know":dk
    }
        
    _replace_vals(df,schema,value_labels,replace_missing_vals)
    _delete_metadata_vals(df,schema,value_labels,["Under 18"])
    return df,schema,value_labels

def survey7(df,schema,value_labels):
    """Clean Survey 7 data."""

    # move covidyr missing values to enums 
    # (this is due to the fact that we have rule of > 75 is missing val to try and catch wider net)
    years = ['2019',
    '2020',
    '2021',
    '2022']
    schema["missingValues"] = [v for v in schema["missingValues"] if not v in years]
    for field in schema["fields"]:
        if field["name"] == "covidyr":
            field["constraints"] = {"enum":years}
    dk = "DON'T KNOW"
    replace_missing_vals = {'Not sure':dk,
        "Don\u2019t know":dk,
        "I don't know":dk,
        "Unknown":""
    }
    _replace_vals(df,schema,value_labels,replace_missing_vals)
    _delete_metadata_vals(df,schema,value_labels,["Under 18"])
    return df,schema,value_labels

def survey8(df,schema,value_labels):
    """Clean Survey 8 data."""
    dk = "DON'T KNOW"
    replace_missing_vals = {
        "DON\u2019T KNOW":dk,
        "Unknown":""
    }
        
    _replace_vals(df,schema,value_labels,replace_missing_vals)
    _delete_metadata_vals(df,schema,value_labels,["Under 18"])
    return df,schema,value_labels


def survey9(df,schema,value_labels):
    """ Clean Survey 9. """
    dk = "DON'T KNOW"
    replace_missing_vals = {"I don\u2019t recall":dk,
        "I don\u2019t know":dk,
        "I don't know":dk
    }
        
    _replace_vals(df,schema,value_labels,replace_missing_vals)
    _delete_metadata_vals(df,schema,value_labels,["Under 18"])
    return df,schema,value_labels
