# Generate data package

import pandas as pd
import yaml
from datetime import datetime, timezone
from utils import drop_excluded_fields, write_data_package
from frictionless import Schema, Resource, Package
import cleaning

VERSION = '1.0'

resources = []
for w in range(1,7):
    df = (pd.read_csv(f'data/protocol1-omnibus/csv/wave{w}.csv', dtype='object')
          .assign(caseid=lambda x: x.caseid.astype('int'))
          .set_index('caseid', verify_integrity=True)
          .sort_index()
         )
    schema = Schema.from_descriptor(f'metadata/schemas/wave{w}-schema.yaml')
    with open(f'metadata/value_labels/spss/wave{w}-labels.yaml') as f:
        labels = yaml.safe_load(f)

    # Clean data and drop any fields marked exclude_from_release
    f = getattr(cleaning, f'w{w}')
    df = f(df)
    drop_excluded_fields(schema, df)

    rsrc = Resource(name=f'wave{w}', schema=schema, profile='tabular-data-resource')
    rsrc.df = df
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
