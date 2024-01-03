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
    UNSEGMENTED = 'unknown'

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
    PRUNED = 'PRUNED'
    INACTIVE = 'INACTIVE'
    EXACT_MATCH = 'EXACT_MATCH'
    SIMILAR_MATCH = 'SIMILAR_MATCH'
    NOT_MATCH = 'NOT_A_MATCH'
    UNSURE = 'UNSURE'
    UNAUDITED = 'UNAUDITED'
    CONFLICT = 'CONFLICT'

class CUSTOMER_SELLER_TYPE_CONFIG:
    _1P = "_1p"
    _3P = "_3p"
    _1P_OR_3P = "_1p_or_3p"
    _1P_OVER_3P = "_1p_over_3p"

class CUSTOMER_CARDINALITY_CONFIG:
    _1 = "1"
    _N = "n"

class CUSTOMER_MATCH_TYPE_CONFIG:
    EXACT_MATCH = "exact_match"
    SIMILAR_MATCH = "similar_match"
    EXACT_OR_SIMILAR_MATCH = "exact_or_similar_match"
    EXACT_OVER_SIMILAR_MATCH = "exact_over_similar_match" 
