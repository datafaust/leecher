CATEGORIES = [
    # {'term': 'daycare center', 'category': 'childcare'},
    {'term': 'church', 'category': 'religiousorg'}
]

NYC_ZIPS = [11746]

CITY = "New York"

USE_PROXIES = False

RECORD_LIMIT = 50

FORCE_EMAIL_OVERWRITE = False  # Set to True to re-scrape all rows with a real_website for an email

FORCE_WEBSITE_OVERWRITE = False # Set to True to re-scrape all rows with a redirect url for a real website


# urgent cares
# clinics
# weed stores
# physical therapy
# art studio
# gallery
# churches
# drug treatments
# plumbing
# janitorial
# fire alarm
# junk removal
# pest control
# business incubators
# moving companies
# porta potty business
# crime scene clean up
# restaurants 
# repair shops
# funeral homes/moturaries
# taxi garages
# warehouses
# trucking companies


# Define all NYC zip codes
#manhattan_zip_codes = [10001, 10002, 10003, 10004, 10005, 10006, 10007, 10009, 10010,
#    10011, 10012, 10013, 10014, 10016, 10017, 10018, 10019, 10020,
#    10021, 10022, 10023, 10024, 10025, 10026, 10027, 10028, 10029, 10030,
#    10031, 10032, 10033, 10034, 10035, 10036, 10037, 10038, 10039, 10040,
#    10044, 10065, 10075, 10128, 10270, 10271, 10272, 10273, 10274, 10275,
#    10276, 10277, 10278, 10279, 10280, 10281, 10282]

#staten_island_zip_codes = [10301, 10302, 10303, 10304, 10305, 10306, 10307, 10308, 10309, 10310,
#    10311, 10312, 10314]

#bronx_zip_codes = [10451, 10452, 10453, 10454, 10455, 10456, 10457, 10458, 10459, 10460,
#    10461, 10462, 10463, 10464, 10465, 10466, 10467, 10468, 10469, 10470,
#    10471, 10472, 10473, 10474, 10475]

#queens_zip_codes = [11004#, 11005, 11351, 11354, 11355, 11356, 11357, 11358, 11359, 11360,
#     11361, 11362, 11363, 11364, 11365, 
#    11366, 
#    11367, 11368, 11369, 11370,
#    11372, 11373, 11374, 11375, 11377, 11378, 11379, 11385, 11414, 11415,
#    11416, 11417, 11418, 11419, 11420, 11421, 11422, 11423, 11426, 11427,
#    11428, 11429, 11691, 11692, 11693, 11694, 11695, 11696, 11697
#   ]

# brooklyn_zip_codes = [11201, 11202, 11203, 11204, 11205, 11206, 11207, 11208, 11209, 11210,
#    11211, 11212, 11213, 11214, 11215, 11216, 11217, 11218, 11219, 11220,
#    11221, 11222, 11223, 11224, 11225, 11226, 11228, 11229, 11230, 11231,
#    11232, 11233, 11234, 11235, 11236, 11237, 11238, 11239, 11240, 11241,
#    11242, 11243, 11244, 11245, 11246, 11247, 11248, 11249, 11250, 11251,
#    11252, 11253, 11254, 11255, 11256]

# nassau_zip_codes = [11001, 11002, 11003, 11004, 11005, 11010, 11020, 11021, 11030, 11040, 
#                     11042, 11050, 11054, 11096, 11501, 11507, 11509, 11510, 11514, 11516, 
#                     11518, 11520, 11530, 11542, 11545, 11548, 11550, 11552, 11553, 11554, 
#                     11557, 11560, 11563, 11565, 11566, 11568, 11570, 11572, 11575, 11576, 
#                     11577, 11580, 11581, 11590, 11593, 11595, 11691, 11692, 11693]

# suffolk_zip_codes = [11701, 11702, 11703, 11704, 11705, 11706, 11707, 11710, 11713, 11714, 
#                      11715, 11716, 11717, 11718, 11719, 11720, 11721, 11722, 11724, 11725, 
#                      11726, 11727, 11729, 11730, 11731, 11732, 11733, 11735, 11736, 11737, 
#                      11738, 11739, 11740, 11741, 11743, 11746, 11747, 11749, 11750, 11751, 
#                      11752, 11753, 11754, 11755, 11757, 11758, 11762, 11763, 11764, 11767, 
#                      11768, 11769, 11770, 11771, 11772, 11773, 11775, 11776, 11777, 11778]


# business_targets = [
#     # {'term': 'real estate agency', 'category': 'realestate'},
#     # {'term': 'law office', 'category': 'lawyers'},
#     # {'term': 'accounting firm', 'category': 'accountants'},
#     # {'term': 'medical clinic', 'category': 'health'},
#     # {'term': 'dental office', 'category': 'dentists'},
#     # {'term': 'chiropractor', 'category': 'chiropractors'},
#     # {'term': 'veterinarian', 'category': 'vet'},
#     # {'term': 'daycare center', 'category': 'childcare'},
#     # {'term': 'private school', 'category': 'education'},
#     # {'term': 'consulting firm', 'category': 'professional'},
#     # {'term': 'insurance agency', 'category': 'insurance'},
#     # {'term': 'construction company', 'category': 'contractors'},
#     # {'term': 'plumbing company', 'category': 'plumbing'},
#     # {'term': 'electrical contractor', 'category': 'electricians'},
#     # {'term': 'car repair shop', 'category': 'autorepair'},
#     # {'term': 'IT services', 'category': 'itservices'},
#     # {'term': 'security services', 'category': 'securityservices'},
#     # {'term': 'cleaning service', 'category': 'homecleaning'},
#     # {'term': 'marketing agency', 'category': 'marketing'},
#     {'term': 'nonprofit organization', 'category': 'nonprofit'}
# ]




