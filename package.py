# Generate data package

import pandas as pd
import yaml
from datetime import datetime, timezone
from utils import drop_excluded_fields, write_data_package
from frictionless import Schema, Resource, Package
import cleaning

VERSION = '2.0'
DATASETS = [{'name':'survey1',
             'filename':'omnibus_survey1_2020-02_072821'},
            {'name':'survey2',
             'filename':'omnibus_survey2_2020-04_072821'},
            {'name':'survey3',
             'filename':'omnibus_survey3_2020-06_072821'},
            {'name':'survey4',
             'filename':'omnibus_survey4_2020-10_072821'},
            {'name':'survey5',
             'filename':'omnibus_survey5_2021-02_072821'},
            {'name':'survey7',
             'filename':'omnibus_survey7_2022-04'},
            {'name':'survey8',
             'filename':'omnibus_survey8_2022-12'}]

resources = []
for dataset in DATASETS:
    df = (pd.read_csv(f'data/protocol1-omnibus/csv/{dataset["filename"]}.csv', dtype='object')
          .assign(caseid=lambda x: x.caseid.astype('int'))
          .set_index('caseid', verify_integrity=True)
          .sort_index()
         )
    schema = Schema.from_descriptor(f'metadata/schemas/{dataset["filename"]}-schema.yaml')
    with open(f'metadata/value_labels/spss/{dataset["filename"]}-labels.yaml') as f:
        labels = yaml.safe_load(f)

    # Clean data and drop any fields marked exclude_from_release
    f = getattr(cleaning, dataset['name'])
    df = f(df)
    drop_excluded_fields(schema, df)

    rsrc = Resource(df, name=dataset['name'], schema=schema,
                    profile='tabular-data-resource')
    rsrc.value_labels = labels
    resources.append(rsrc)

package = Package(
    name='jcoin-protocol1-surveys',
    title='JCOIN Opioid Stigma Surveys',
    profile='tabular-data-package',
    version=VERSION,
    homepage='https://healdata.org/portal/discovery/1U2CDA050098-01_a',
    created=datetime.now(timezone.utc).astimezone().isoformat(),
    resources = resources
)

write_data_package(package, f'tmp/jcoin-protocol1-surveys-v{VERSION}')
