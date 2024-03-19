# JCOIN Stigma Surveys Data Packaging

Schemas and files containing value labels for each wave may be generated from
within Stata using:

```
forv i = 1/6 {
    to_frictionless using data/protocol1-omnibus/wave`i'.sav, clear
}
```
