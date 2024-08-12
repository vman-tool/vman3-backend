

class db_collections():
    """
    Constants class for NoSQL database Collections. Use this class to call your collection
    """
    VA_TABLE: str = 'form_submissions'
    USERS: str = 'users'
    USER_TOKENS: str = 'user_tokens'
    USER_ROLES: str = 'role'
    ICD10_CATEGORY: str = 'icd10_category'
    ICD10: str = 'icd10'
    ASSIGNED_VA: str = 'assigned_va'
    CODED_VA: str = 'coded_va'
    DOWNLOAD_TRACKER: str   ='download_tracker'
    DOWNLOAD_PROCESS_TRACKER: str   ='download_process_tracker'
    SYSTEM_CONFIGS: str = 'system_configs'
    
    
    
collections_with_indexes = {
    # db_collections.VA_TABLE: [
    #    {"fields": ["id10005r"], "unique": False, "type": "persistent", "name": "idx_region"}
    # ],
    db_collections.VA_TABLE: [
        {"fields": ["id10005r"], "unique": False, "type": "persistent", "name": "idx_region"},
        {"fields": ["id10012"], "unique": False, "type": "persistent", "name": "idx_date"},
        {"fields": ["today"], "unique": False, "type": "persistent", "name": "idx_submission"},
        # {"fields": ["age_group"], "unique": False, "type": "persistent", "name": "idx_age_group"},
        {"fields": ["id10007"], "unique": False, "type": "persistent", "name": "idx_interviewer"}
    ],
    db_collections.USERS: [],
    db_collections.USER_TOKENS: [],
    db_collections.USER_ROLES: [],
    db_collections.ICD10_CATEGORY: [],
    db_collections.ICD10: [],
    db_collections.ASSIGNED_VA: [],
    db_collections.CODED_VA: [],
    db_collections.DOWNLOAD_TRACKER: [],
    db_collections.DOWNLOAD_PROCESS_TRACKER: [],
    db_collections.SYSTEM_CONFIGS: []
}