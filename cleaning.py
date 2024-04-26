# Data cleaning

def survey1(df):
    """Clean Survey 1 data."""

    return (df
            .assign(employ=df.employ.str.replace('–','-'))
    )

def survey2(df):
    """Clean Survey 2 data."""

    return (df
            .assign(employ=df.employ.str.replace('–','-'))
            .replace({'familyuse_ever':{'Don’t know':"DON'T KNOW"}})
    )

def survey3(df):
    """Clean Survey 3 data."""

    return (df
            .assign(employ=df.employ.str.replace('–','-'))
            .replace({'personaluse_last':{'6 – 12 months ago':'6-12 months ago'},
                      'duration_norcnd':{'Under 1 minute':'0'}})
    )

def survey4(df):
    """Clean Survey 4 data."""

    return (df
            .assign(employ=df.employ.str.replace('–','-'))
            .replace({'familyuse_ever':{'0':'No'}})
    )

def survey5(df):
    """Clean Survey 5 data."""

    return (df
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
                      'personaluse_last':{'7 – 12 months ago':'7-12 months ago'},
                      'pidi':{"Don’t lean":"Don't lean"}}
                    )
    )

def survey7(df):
    """Clean Survey 7 data."""

    return df

def survey8(df):
    """Clean Survey 8 data."""

    return df
