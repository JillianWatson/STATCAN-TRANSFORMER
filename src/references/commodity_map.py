# ============================================================
# COMMODITY LOOKUP DICTIONARY
# Maps raw StatCan commodity strings to clean category labels.
#
# Notes:
#   - 3-part HS codes (e.g. 0101.21.00) are export classifications
#   - 4-part HS codes (e.g. 0101.21.00.00) are import classifications
#   - Both formats map to the same label; Flow_Type keeps them distinct
#   - None values indicate out-of-scope commodities to be excluded
# ============================================================

COMMODITY_MAP = {
    # EQUINE
    "0101.21.00 - Horses, live, pure-bred breeding":"horse_breeding",
    "0101.21.00.00 - Horses, live, pure-bred breeding":"horse_breeding",
    "0101.29.00.10 - Horses, live, for slaughter":"horse_slaughter",
    "0101.29.00.20 - Horses for racing":"horse_racing",
    "0101.29.00.90 - Horses, live, other than pure-bred breeding, o/t for slaughter or racing,nes":"horse_other",
    "0101.29.90 - Horses, live, other than pure-bred or for slaughter":"horse_other",
    "0101.30.00 - Asses, live":"ass",
    "0101.30.00.00 - Asses, live":"ass",
    "0101.90.00 - Mules and hinnies, live":"mule_hinny",
    "0101.90.00.00 - Mules and hinnies, live":"mule_hinny",

    # BOVINE: DAIRY
    "0102.21.00.10 - Cattle, live, pure-bred breeding, dairy":"cattle_dairy_purebred",
    "0102.21.11 - Cattle, live, dairy, male, pure-bred breeding":"cattle_dairy_male_purebred",
    "0102.21.12 - Cattle, live, dairy, female, pure-bred breeding":"cattle_dairy_female_purebred",
    "0102.29.00.10 - Cattle, live, dairy, other than pure-bred breeding":"cattle_dairy_crossbred",
    "0102.29.12 - Cattle, live, other than pure-bred breeding, dairy, weighing 90 kg or more":"cattle_dairy_crossbred",

    # BOVINE: BREEDING
    "0102.21.00.90 - Cattle, live, pure-bred breeding, o/t dairy, nes":"cattle_breeding_purebred",
    "0102.21.91 - Cattle, male, live, pure-bred breeding, other than dairy":"cattle_breeding_male_purebred",
    "0102.21.92 - Cattle, female, live, pure-bred breeding, other than dairy":"cattle_breeding_female_purebred",
    "0102.29.60 - Cattle, live, for breeding, weighing >= 320 kg, o/t pure-bred":"cattle_breeding_crossbred",

    # BOVINE: FEEDER
    "0102.29.20 - Cattle, live, weighing <= 200 kg, o/t pure-bred for breeding or dairy":"cattle_feeder_calf",
    "0102.29.41 - Cattle, male, live, o/t pure-bred breeding,o/t dairy,weighing >= 200 kg < 320 kg":"cattle_feeder_male_backgrounder",
    "0102.29.42 - Cattle, female,live,o/t pure-bred breeding,o/t dairy,weighing >= 200 kg < 320 kg":"cattle_feeder_female_backgrounder",

    # BOVINE: SLAUGHTER
    "0102.29.51 - Steers, for immediate slaughter, wt >= 320 kg":"cattle_slaughter_steer",
    "0102.29.52 - Bulls, for immediate slaughter, wt >= 320 kg":"cattle_slaughter_bull",
    "0102.29.53 - Cows, for immediate slaughter, wt >= 320 kg":"cattle_slaughter_cow",
    "0102.29.54 - Heifers, for immediate slaughter,wt >= 320 kg":"cattle_slaughter_heifer",

    # BOVINE: OTHER 
    "0102.29.00.90 - Cattle, live, other than pure-bred breeding and other than dairy, nes":"cattle_other",
    "0102.29.91 - Other cattle, male, weighing >= 320 kg, nes":"cattle_other_male",
    "0102.29.92 - Other cattle, female, weighing >= 320 kg, nes":"cattle_other_female",

    # EXCLUDED (out of scope)
    "0102.31.00 - Buffalo, live, pure-bred breeding":None,
    "0102.39.10 - Bison, live":None,
    "0102.90.00 - Bovine, live, other than cattle and buffalo, nes":None,
    "0102.90.00.00 - Bovine, live, other than cattle and buffalo, nes":None,
}

