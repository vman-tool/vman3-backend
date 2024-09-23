

class db_collections():
    """
    Constants class for NoSQL database Collections. Use this class to call your collection
    """
    VA_TABLE: str = 'form_submissions'
    VA_QUESTIONS: str = 'form_questions'
    USERS: str = 'users'
    USER_TOKENS: str = 'user_tokens'
    ROLES: str = 'role'
    USER_ROLES: str = 'user_role'
    ICD10_CATEGORY: str = 'icd10_category'
    ICD10: str = 'icd10'
    ASSIGNED_VA: str = 'assigned_va'
    CODED_VA: str = 'coded_va'
    DOWNLOAD_TRACKER: str   ='download_tracker'
    DOWNLOAD_PROCESS_TRACKER: str   ='download_process_tracker'
    SYSTEM_CONFIGS: str = 'system_configs'
    # INTERVA5: str = 'interva_'
    CCVA_RESULTS: str = 'ccva_results'
    CCVA_GRAPH_RESULTS: str = 'ccva_graph_results'
    CCVA_ERRORS:str = 'ccva_errors'


class AccessPrivileges():
    """
    Access privileges for different entities
    """
    PCVA_MODULE_ACCESS: str = 'PCVA_VIEW'
    PCVA_CREATE_ICD10_CODES: str = 'PCVA_CREATE_ICD10_CODES'
    PCVA_VIEW_ICD10_CODES: str = 'PCVA_VIEW_ICD10_CODES'
    PCVA_UPDATE_ICD10_CODES: str = 'PCVA_UPDATE_ICD10_CODES'
    PCVA_DELETE_ICD10_CODES: str = 'PCVA_DELETE_ICD10_CODES'
    PCVA_CREATE_ICD10_CATEGORIES: str = 'PCVA_CREATE_ICD10_CATEGORIES'
    PCVA_VIEW_ICD10_CATEGORIES: str = 'PCVA_VIEW_ICD10_CATEGORIES'
    PCVA_UPDATE_ICD10_CATEGORIES: str = 'PCVUPDATETE_ICD10_CATEGORIES'
    PCVA_DELETE_ICD10_CATEGORIES: str = 'PCVDELETETE_ICD10_CATEGORIES'
    
    
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
    db_collections.ROLES: [],
    db_collections.USER_ROLES: [],
    db_collections.USER_TOKENS: [],
    db_collections.USER_ROLES: [],
    db_collections.ICD10_CATEGORY: [],
    db_collections.ICD10: [],
    db_collections.ASSIGNED_VA: [],
    db_collections.CODED_VA: [],
    db_collections.DOWNLOAD_TRACKER: [],
    db_collections.DOWNLOAD_PROCESS_TRACKER: [],
    db_collections.SYSTEM_CONFIGS: [],
    db_collections.VA_QUESTIONS: [],
    db_collections.CCVA_RESULTS: [
        #   {"fields": ["ID"], "unique": True, "type": "persistent", "name": "idx_interva5_id"},
          ],
    db_collections.CCVA_GRAPH_RESULTS: [
        
    ],
    db_collections.CCVA_ERRORS: [],
}