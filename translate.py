from to_frictionless import to_frictionless

PROTOCOL1_SURVEYS = [
"omnibus_survey1_2020-02_072821.sav",
"omnibus_survey2_2020-04_072821.sav",
"omnibus_survey3_2020-06_072821.sav",
"omnibus_survey4_2020-10_072821.sav",
"omnibus_survey5_2021-02_072821.sav",
"omnibus_survey7_2022-04.sav",
"omnibus_survey8_2022-12.sav",
"omnibus_survey9_2023-05.sav"
]

for filename in PROTOCOL1_SURVEYS:
    basepath = "data/protocol1-omnibus/"
    to_frictionless(basepath+filename)