# Generate combined file for use in updating VLMD

import yaml
import csv
from contextlib import suppress

SURVEYS = {1:'omnibus_survey1_2020-02_072821-schema.yaml',
           2:'omnibus_survey2_2020-04_072821-schema.yaml',
           3:'omnibus_survey3_2020-06_072821-schema.yaml',
           4:'omnibus_survey4_2020-10_072821-schema.yaml',
           5:'omnibus_survey5_2021-02_072821-schema.yaml',
           7:'omnibus_survey7_2022-04-schema.yaml',
           8:'omnibus_survey8_2022-12-schema.yaml',
           9:'omnibus_survey9_2023-05-schema.yaml'}

fields = {}
for s in SURVEYS:
    with open(f'metadata/schemas/{SURVEYS[s]}') as f:
        flds = yaml.safe_load(f)['fields']
        for f in flds:
            if f['name'] in fields:
                fields[f['name']]['surveys'].append(s)
            else:
                fields[f['name']] = f
                fields[f['name']]['surveys'] = [s]
                with suppress(KeyError):
                    fields[f['name']]['exclude_from_release'] = f['custom']['jcoin']['exclude_from_release']

with open('protocol1_data_dictionary.csv', 'w') as csvfile:
    fieldnames = ['name','surveys','exclude_from_release','title','description']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
    writer.writeheader()
    for f in fields:
        writer.writerow(fields[f])
