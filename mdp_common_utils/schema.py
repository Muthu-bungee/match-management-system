from pyspark.sql.types import *

class MATCH_WAREHOUSE:
    class PAIR_ID:
        NAME = "pair_id"
        DATATYPE = StringType()
        NULLABLE = False
    class SEGMENT:
        NAME = "segment"
        DATATYPE = StringType()
        NULLABLE = False
    class BASE_SKU_UUID:
        NAME = "base_sku_uuid"
        DATATYPE = StringType()
        NULLABLE = False
    class COMP_SKU_UUID:
        NAME = "comp_sku_uuid"
        DATATYPE = StringType()
        NULLABLE = False
    class BASE_SOURCE_STORE:
        NAME = "base_source_store"
        DATATYPE = StringType()
        NULLABLE = False
    class COMP_SOURCE_STORE:
        NAME = "comp_source_store"
        DATATYPE = StringType()
        NULLABLE = False
    class MATCH_SOURCE_SCORE_MAP:
        NAME = "match_source_score_map"
        DATATYPE = MapType(StringType(), DoubleType())
        NULLABLE = False
    class AGGREGATED_SCORE:
        NAME = "aggregated_score"
        DATATYPE = DoubleType()
        NULLABLE = False
    class BUNGEE_AUDIT_STATUS:
        NAME = "bungee_audit_status"
        DATATYPE = StringType()
        NULLABLE = True
    class MISC_INFO:
        NAME = "misc_info"
        DATATYPE = StringType()
        NULLABLE = True
    class BUNGEE_AUDITOR:
        NAME = "bungee_auditor"
        DATATYPE = StringType()
        NULLABLE = True
    class BUNGEE_AUDIT_DATE:
        NAME = "bungee_audit_date"
        DATATYPE = DateType()
        NULLABLE = True
    class BUNGEE_AUDITOR_COMMENT:
        NAME = "bungee_auditor_comment"
        DATATYPE = StringType()
        NULLABLE = True
    class SELLER_TYPE:
        NAME = "seller_type"
        DATATYPE = StringType()
        NULLABLE = True
    class CLIENT_AUDIT_STATUS_L1:
        NAME = "client_audit_status_l1"
        DATATYPE = StringType()
        NULLABLE = True
    class CLIENT_AUDITOR_L1:
        NAME = "client_auditor_l1"
        DATATYPE = StringType()
        NULLABLE = True
    class CLIENT_AUDIT_DATE_L1:
        NAME = "client_audit_date_l1"
        DATATYPE = DateType()
        NULLABLE = True
    class CLIENT_AUDITOR_L1_COMMENT:
        NAME = "client_auditor_l1_comment"
        DATATYPE = StringType()
        NULLABLE = True
    class CLIENT_AUDIT_STATUS_L2:
        NAME = "client_audit_status_l2"
        DATATYPE = StringType()
        NULLABLE = True
    class CLIENT_AUDITOR_L2:
        NAME = "client_auditor_l2"
        DATATYPE = StringType()
        NULLABLE = True
    class CLIENT_AUDIT_DATE_L2:
        NAME = "client_audit_date_l2"
        DATATYPE = DateType()
        NULLABLE = True
    class CLIENT_AUDITOR_L2_COMMENT:
        NAME = "client_auditor_l2_comment"
        DATATYPE = StringType()
        NULLABLE = True
    class CREATED_DATE:
        NAME = "created_date"
        DATATYPE = DateType()
        NULLABLE = False
    class CREATED_BY:
        NAME = "created_by"
        DATATYPE = StringType()
        NULLABLE = False
    class UPDATED_DATE:
        NAME = "updated_date"
        DATATYPE = DateType()
        NULLABLE = True
    class UPDATED_BY:
        NAME = "updated_by"
        DATATYPE = StringType()
        NULLABLE = True
    class WORKFLOW_NAME:
        NAME = "workflow_name"
        DATATYPE = StringType()
        NULLABLE = True
    

def get_column_list(schema):
    key_list = []
    for attr_name in dir(schema):
        if not attr_name.startswith("__"):
            sub_class = schema.__dict__[attr_name]
            key = sub_class.__getattribute__(sub_class, "NAME")
            key_list.append(key)
    return key_list