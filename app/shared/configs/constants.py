from functools import lru_cache

class db_collections():
    """
    Constants class for NoSQL database Collections. Use this class to call your collection
    """
    VA_TABLE: str = 'form_submissions'
    VA_QUESTIONS: str = 'form_questions'
    USERS: str = 'users'
    USER_TOKENS: str = 'user_tokens'
    ROLES: str = 'role'
    USER_ROLES: str = 'user_roles'
    USER_ACCESS_LIMIT: str = 'user_access_limit'
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
    db_collections.USER_ACCESS_LIMIT: [],
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

class AccessPrivileges():
    """
        Access privileges for different entities
    """

    # PCVA Privileges
    PCVA_MODULE_ACCESS: str = 'PCVA_MODULE_VIEW'
    PCVA_CREATE_ICD10_CODES: str = 'PCVA_CREATE_ICD10_CODES'
    PCVA_VIEW_ICD10_CODES: str = 'PCVA_VIEW_ICD10_CODES'
    PCVA_UPDATE_ICD10_CODES: str = 'PCVA_UPDATE_ICD10_CODES'
    PCVA_DELETE_ICD10_CODES: str = 'PCVA_DELETE_ICD10_CODES'
    PCVA_CREATE_ICD10_CATEGORIES: str = 'PCVA_CREATE_ICD10_CATEGORIES'
    PCVA_VIEW_ICD10_CATEGORIES: str = 'PCVA_VIEW_ICD10_CATEGORIES'
    PCVA_UPDATE_ICD10_CATEGORIES: str = 'PCVA_UPDATE_ICD10_CATEGORIES'
    PCVA_DELETE_ICD10_CATEGORIES: str = 'PCVA_DELETE_ICD10_CATEGORIES'

    # USERS PRIVILEGES
    USERS_MODULE_VIEW: str = 'USERS_MODULE_VIEW'
    USERS_CREATE_USER: str = 'USERS_CREATE_USER'
    USERS_UPDATE_USER: str = 'USERS_UPDATE_USER'
    USERS_VIEW: str = 'USERS_VIEW'
    USERS_DEACTIVATE_USER: str = 'USERS_DEACTIVATE_USER'
    USERS_ASSIGN_ROLES: str = 'USERS_ASSIGN_ROLES'
    USERS_CREATE_ROLES: str = 'USERS_CREATE_ROLES'
    USERS_DELETE_ROLES: str = 'USERS_DELETE_ROLES'
    USERS_UPDATE_ROLES: str = 'USERS_UPDATE_ROLES'
    USERS_VIEW_ROLES: str = 'USERS_VIEW_ROLES'
    USERS_VIEW_PRIVILEGES: str = 'USERS_VIEW_PRIVILEGES'
    USERS_LIMIT_DATA_ACCESS: str = 'USERS_LIMIT_DATA_ACCESS'
    USERS_UPDATE_ACCESS_LIMIT_LABELS: str = 'USERS_UPDATE_ACCESS_LIMIT_LABELS'

    #ODK
    ODK_MODULE_VIEW: str = 'ODK_MODULE_VIEW'
    ODK_DATA_SYNC: str = 'ODK_DATA_SYNC'
    ODK_QUESTIONS_SYNC: str = 'ODK_QUESTIONS_SYNC'

    #SUBMISSIONS
    SUBMISSIONS_DATA_VIEW: str = 'SUBMISSIONS_DATA_VIEW'

    #SETTINGS
    SETTINGS_MODULE_VIEW: str = 'SETTINGS_MODULE_VIEW'
    SETTINGS_CONFIGS_VIEW: str = 'SETTINGS_CONFIGS_VIEW'
    SETTINGS_CREATE_ODK_DETAILS: str = 'SETTINGS_CREATE_ODK_DETAILS'
    SETTINGS_UPDATE_ODK_DETAILS: str = 'SETTINGS_UPDATE_ODK_DETAILS'
    SETTINGS_VIEW_ODK_DETAILS: str = 'SETTINGS_VIEW_ODK_DETAILS'
    SETTINGS_CREATE_SYSTEM_CONFIGS: str = 'SETTINGS_CREATE_SYSTEM_CONFIGS'
    SETTINGS_UPDATE_SYSTEM_CONFIGS: str = 'SETTINGS_UPDATE_SYSTEM_CONFIGS'
    SETTINGS_VIEW_SYSTEM_CONFIGS: str = 'SETTINGS_VIEW_SYSTEM_CONFIGS'
    SETTINGS_CREATE_FIELD_MAPPING: str = 'SETTINGS_CREATE_FIELD_MAPPING'
    SETTINGS_UPDATE_FIELD_MAPPING: str = 'SETTINGS_UPDATE_FIELD_MAPPING'
    SETTINGS_VIEW_FIELD_MAPPING: str = 'SETTINGS_VIEW_FIELD_MAPPING'
    SETTINGS_CREATE_VA_SUMMARY: str = 'SETTINGS_CREATE_VA_SUMMARY'
    SETTINGS_UPDATE_VA_SUMMARY: str = 'SETTINGS_UPDATE_VA_SUMMARY'
    SETTINGS_VIEW_VA_SUMMARY: str = 'SETTINGS_VIEW_VA_SUMMARY'


    @classmethod
    @lru_cache(maxsize=None)
    def get_privileges(cls, search_term: str=None, exact: bool=False):
        """
            Get all privileges or filter based on search term and exact match flag.
            :param search_term: Search term for filtering privileges.
            :param exact: Flag to specify exact match or partial search.
            :return: List of privileges.
        """
        if search_term:
            return [value for name, value in vars(cls).items() if isinstance(value, str) and not name.startswith("__") and (search_term == value if exact else search_term.lower() in value.lower())]
        return [value for name, value in vars(cls).items() if isinstance(value, str) and not name.startswith("__")]
 