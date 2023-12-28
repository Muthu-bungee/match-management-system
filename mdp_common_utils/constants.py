# Column Name
MATCH_SEGMENT = 'segment'
MATCH_TYPE = 'type'
MATCH_SOURCE = 'match_source'

# Match Segment
class SEGMENT :
    PETS_CA = 'petsca'
    PETS = 'pets'
    OFFICE_SUPPLIES = 'office_supplies'
    GROCERY = 'grocery'
    SPORTS_OUTDOORS = 'sports_outdoors'
    UNSEGMENTED = 'rejected'

# Match type
EXACT_MATCH = 'exact_match'
SIMILAR_MATCH = 'similar_match'
NOT_MATCH = 'not_a_match'
UNSURE = 'unsure'

# Match Source
class MATCH_SOURCE :
    FASTLANE = 'fastlane'
    ML = 'ml'
    UPC = 'upc'
    MPN = 'mpn'
    LEGACY = 'legacy'
    MANUAL = 'manual'
    LEGACY_ML ='legacy_ml'
    TRANSITIVE = 'transitive'
    FASTLANE_CUSTOMER_REJECTED = "fastlane_customer_scrape_rejected"

class BUNGEE_AUDIT_STATUS :
    INACTIVE = 'INACTIVE'
    EXACT_MATCH = 'EXACT_MATCH'
    SIMILAR_MATCH = 'SIMILAR_MATCH'
    NOT_MATCH = 'NOT_A_MATCH'
    UNSURE = 'UNSURE'
    UNAUDITED = 'UNAUDITED'
    CONFLICT = 'CONFLICT'
