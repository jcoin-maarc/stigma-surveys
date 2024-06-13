# JCOIN Stigma Surveys Data Packaging

Schemas and files containing value labels for each wave were generated from `pyreadstat` module


## Usage

`pip install -r requirements.txt` in a virtual env.
`dvc pull`
Run python files:
`crosswalkxlsx_to_mappings.py` : creates metadata mappings from an excel file that was manually annotated by NORC Stigma Survey team with support/feedback from DASC team.
`translate.py`: uses the source `spss` files and the mappings to generate schemas, data, and metadata and performs curation on metadata and data from metadata mappings and manual curation from DASC not included in survey team mappings (e.g., unstandardized missing value labels, etc)
`package.py`: packages into a data package
- to validate and save summary to a file, run from the CLI `frictionless validate tmp/omnibus-protocol1-<version> > summary.txt`
- add to `cleaning.py` as necessary -- changing either just the data (if you like how the data is representated by the metadata/labels) or both the data + the metadata.
