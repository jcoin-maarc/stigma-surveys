# Generate data package

import pandas as pd
import yaml
from datetime import datetime, timezone
from utils import write_data_package
from frictionless import Schema, Resource, Package
import cleaning

resources = []
for w in range(1,7):
    df = (pd.read_csv(f'data/protocol1-omnibus/csv/wave{w}.csv', dtype='object')
          .set_index('caseid', verify_integrity=True)
          .sort_index()
         )
    schema = Schema.from_descriptor(f'metadata/schemas/wave{w}-schema.yaml')
    with open(f'metadata/value_labels/spss/wave{w}-labels.yaml') as f:
        labels = yaml.safe_load(f)

    rsrc = Resource(name=f'wave{w}', schema=schema, profile='tabular-data-resource')
    f = getattr(cleaning, f'w{w}')
    rsrc.df = f(df)
    rsrc.value_labels = labels
    resources.append(rsrc)

package = Package(
    name='jcoin-protocol1-surveys',
    title='JCOIN Opioid Stigma Surveys',
    profile='tabular-data-package',
    version='1.0',
    homepage='https://healdata.org/portal/discovery/1U2CDA050098-01_a',
    created=datetime.now(timezone.utc).astimezone().isoformat(),
    resources = resources
)

write_data_package(package)
