// Translate SPSS file into CSV format, with schema and value labels

prog def to_frictionless

    version 18.0
    syntax using/ [, case(passthru) clear replace]

    mata: st_local("filename", pathrmsuffix(pathbasename(`"`using'"')))
    if `"`case'"' == "" {
        loc case "case(lower)"
    }

    import spss `using', `case' `clear'

    cap mkdir "tmp"
    cap mkdir "tmp/`filename'"
    export delim using "tmp/`filename'/`filename'.csv", delim(",") `replace'

    python: from utils import write_schema
    python: write_schema("""tmp/`filename'/`filename'-schema.yaml""",  ///
                         """tmp/`filename'/`filename'-labels.yaml""")

end
