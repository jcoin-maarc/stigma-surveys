# Data cleaning

def w1(df):
    """Clean Wave 1 data."""

    return df

def w2(df):
    """Clean Wave 2 data."""

    return df

def w3(df):
    """Clean Wave 3 data."""

    return df

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
                                               '99':'REFUSED'}}
                    )
    )

def w6(df):
    """Clean Wave 6 data."""

    return df
