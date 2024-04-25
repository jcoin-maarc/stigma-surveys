# Data cleaning

def survey1(df):
    """Clean Survey 1 data."""

    return (df
            .assign(employ=df.employ.str.replace('–','-'))
    )

def w2(df):
    """Clean Wave 2 data."""

    return (df
            .replace({'familyuse_ever':{'Don’t know':"DON'T KNOW"}})
    )

def w3(df):
    """Clean Wave 3 data."""

    return(df
           .replace({'personaluse_last':{'6 – 12 months ago':'6-12 months ago'},
                     'duration_norcnd':{'Under 1 minute':'0'}})
    )

def w4(df):
    """Clean Wave 4 data."""

    return (df
            .replace({'familyuse_ever':{'0':'No'}})
    )

def w5(df):
    """Clean Wave 5 data."""

    return (df
            .replace({'personalmisuse_lifetime':{'77':"DON'T KNOW",
                                                 '98':'SKIPPED ON WEB',
                                                 '99':'REFUSED'},
                      'famfriendmisuse_lifetime':{'77':"DON'T KNOW",
                                                  '98':'SKIPPED ON WEB',
                                                  '99':'REFUSED'},
                      'personalmisuse_recent':{'77':"DON'T KNOW",
                                               '98':'SKIPPED ON WEB',
                                               '99':'REFUSED'},
                      'personaluse_last':{'7 – 12 months ago':'7-12 months ago'},
                      'pidi':{"Don’t lean":"Don't lean"}}
                    )
    )

def w6(df):
    """Clean Wave 6 data."""

    return df
