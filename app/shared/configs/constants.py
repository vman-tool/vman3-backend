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
    ICD10_CATEGORY_TYPE: str = 'icd10_category_type'
    ICD10_CATEGORY: str = 'icd10_category'
    ICD10: str = 'icd10'
    ASSIGNED_VA: str = 'assigned_va'
    PCVA_RESULTS: str = 'pcva_results'
    PCVA_MESSAGES: str = 'pcva_messages'
    PCVA_CONFIGURATION: str = 'pcva_configuration'
    DOWNLOAD_TRACKER: str   ='download_tracker'
    DOWNLOAD_PROCESS_TRACKER: str   ='download_process_tracker'
    SYSTEM_CONFIGS: str = 'system_configs'
    # INTERVA5: str = 'interva_'
    CCVA_RESULTS: str = 'ccva_results'
    CCVA_GRAPH_RESULTS: str = 'ccva_graph_results'
    CCVA_ERRORS:str = 'ccva_errors'
    CCVA_ERRORS_CORRECTIONS:str = 'ccva_errors_corrections'
    CCVA_PUBLIC_RESULTS: str = 'ccva_public_results'  # Single collection for all public CCVA data (temporary with TTL)
    TASK_PROGRESS: str = 'task_progress'

class Special_Constants():
    UPLOAD_FOLDER: str = '/uploads'
    FILE_URL: str = '/vman/api/v1/uploads'


    
collections_with_indexes = {
    # db_collections.VA_TABLE: [
    #    {"fields": ["id10005r"], "unique": False, "type": "persistent", "name": "idx_region"}
    # ],
    db_collections.VA_TABLE: [
        {"fields": ["__id"], "unique": True, "type": "persistent", "name": "idx___id"},
         {"fields": ["vman_data_source"], "unique": False, "type": "persistent", "name": "idx_vman_data_source"},
        {"fields": ["vman_data_name"], "unique": False, "type": "persistent", "name": "idx_vman_data_name"},
        {"fields": ["__id", "vman_data_source"], "unique": True, "type": "persistent", "name": "idx___id_vman_data_source"},
        
        {"fields": ["id10005r"], "unique": False, "type": "persistent", "name": "idx_region"},
        {"fields": ["id10012"], "unique": False, "type": "persistent", "name": "idx_date"},
        {"fields": ["today"], "unique": False, "type": "persistent", "name": "idx_submission"},
        
        # New Optimization Indexes
        {"fields": ["id10005d"], "unique": False, "type": "persistent", "name": "idx_district"},
        {"fields": ["id10023"], "unique": False, "type": "persistent", "name": "idx_death_date"},
        {"fields": ["id10005r", "today"], "unique": False, "type": "persistent", "name": "idx_region_submission"},
        {"fields": ["id10005d", "today"], "unique": False, "type": "persistent", "name": "idx_district_submission"},
        {"fields": ["id10005r", "id10005d"], "unique": False, "type": "persistent", "name": "idx_region_district"},
        
        # {"fields": ["age_group"], "unique": False, "type": "persistent", "name": "idx_age_group"},
        {"fields": ["id10007"], "unique": False, "type": "persistent", "name": "idx_interviewer"}
    ],
    db_collections.USERS: [
         {"fields": ["is_active"], "type": "persistent", "name": "u_is_active"}
    ],
    db_collections.ROLES: [
        {"fields": ["is_deleted"], "type": "persistent", "name": "r_is_active"},
        {"fields": ["name"], "unique": True, "type": "persistent", "name": "role_name"} ###TODO: Uncomment this line after testing
    ],
    db_collections.USER_ROLES: [
        {"fields": ["is_deleted"], "type": "persistent", "name": "ur_is_active"},
        {"fields": ["user"], "unique": False, "type": "persistent", "name": "role_user"},
        {"fields": ["role"], "unique": False, "type": "persistent", "name": "role"}
    ],
    db_collections.USER_TOKENS: [
        {"fields": ["is_deleted"], "type": "persistent", "name": "ut_is_active"}
    ],
    db_collections.USER_ACCESS_LIMIT: [
        {"fields": ["is_deleted"], "type": "persistent", "name": "ucl_is_active"}
    ],
    db_collections.ICD10_CATEGORY_TYPE: [
        {"fields": ["is_deleted"], "type": "persistent", "name": "ict_is_active"},
        {"fields": ["name"], "unique": True, "type": "persistent", "name": "category_type_name"}
    ],
    db_collections.ICD10_CATEGORY: [
        {"fields": ["is_deleted"], "type": "persistent", "name": "ic_is_active"},
        {"fields": ["name"], "unique": True, "type": "persistent", "name": "category_name"},
        {"fields": ["type"], "unique": False, "type": "persistent", "name": "category_type"}
    ],
    db_collections.ICD10: [
        {"fields": ["is_deleted"], "type": "persistent", "name": "i_is_active"}
    ],
    db_collections.ASSIGNED_VA: [
        {"fields": ["is_deleted"], "type": "persistent", "name": "av_is_active"}
    ],
    db_collections.PCVA_RESULTS: [
        {"fields": ["is_deleted"], "type": "persistent", "name": "cv_is_active"},
        {"fields": ["va"], "unique": False, "type": "persistent", "name": "va_record"}
    ],
    db_collections.PCVA_MESSAGES: [
        {"fields": ["created_by"], "type": "persistent", "name": "message_sender"},
        {"fields": ["is_deleted"], "type": "persistent", "name": "message_is_deleted"},
        {"fields": ["va"], "unique": False, "type": "persistent", "name": "va_record"}
    ],
    db_collections.PCVA_CONFIGURATION: [
        {"fields": ["created_by"], "type": "persistent", "name": "configuration_creator"},
        {"fields": ["updated_by"], "type": "persistent", "name": "configuration_updator"},
        {"fields": ["is_deleted"], "type": "persistent", "name": "message_is_deleted"},
    ],
    db_collections.DOWNLOAD_TRACKER: [],
    db_collections.DOWNLOAD_PROCESS_TRACKER: [],
    db_collections.SYSTEM_CONFIGS: [],
    db_collections.VA_QUESTIONS: [],
    db_collections.CCVA_RESULTS: [
             {"fields": ["CAUSE1"], "type": "persistent", "name": "cause_idx"}
        #   {"fields": ["ID"], "unique": True, "type": "persistent", "name": "idx_interva5_id"},
          ],
    db_collections.CCVA_GRAPH_RESULTS: [
        
    ],
    db_collections.CCVA_ERRORS: [],
    db_collections.CCVA_ERRORS_CORRECTIONS: [],
    db_collections.CCVA_PUBLIC_RESULTS: [
        {"fields": ["task_id"], "unique": True, "type": "persistent", "name": "idx_task_id"},
        {"fields": ["ttl"], "type": "persistent", "name": "idx_ttl"}
    ],
    db_collections.TASK_PROGRESS: [
        {"fields": ["expires_at"], "type": "persistent", "name": "idx_expires_at", "expireAfter": 0}
    ]
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
    PCVA_CREATE_ICD10_CATEGORY_TYPES: str = 'PCVA_CREATE_ICD10_CATEGORY_TYPES'
    PCVA_VIEW_ICD10_CATEGORY_TYPES: str = 'PCVA_VIEW_ICD10_CATEGORY_TYPES'
    PCVA_UPDATE_ICD10_CATEGORY_TYPES: str = 'PCVA_UPDATE_ICD10_CATEGORY_TYPES'
    PCVA_DELETE_ICD10_CATEGORY_TYPES: str = 'PCVA_DELETE_ICD10_CATEGORY_TYPES'
    PCVA_UPLOAD_CATEGORIES_VIA_FILE: str = 'PCVA_UPLOAD_CATEGORIES_VIA_FILE'
    PCVA_UPLOAD_CODES_VIA_FILE: str = 'PCVA_UPLOAD_CODES_VIA_FILE'
    PCVA_VIEW_VA_RECORDS: str = 'PCVA_VIEW_VA_RECORDS'
    PCVA_VIEW_CODERS: str = 'PCVA_VIEW_CODERS'
    PCVA_CODE_VA: str = 'PCVA_CODE_VA'
    PCVA_ASSIGN_VA: str = 'PCVA_ASSIGN_VA'

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
    SETTINGS_UPDATE_SYSTEM_IMAGES: str = 'SETTINGS_UPDATE_SYSTEM_IMAGES'
    
    #DASHBOARD
    DASHBOARD_MODULE_VIEW: str = 'DASHBOARD_MODULE_VIEW'

    #CCVA
    CCVA_MODULE_VIEW: str = 'CCVA_MODULE_VIEW'
    CCVA_MODULE_RUN: str = 'CCVA_MODULE_RUN'
    CCVA_MODULE_CRUD: str = 'CCVA_MODULE_CRUD'
    CCVA_MODULE_VIEW_RESULTS: str = 'CCVA_MODULE_VIEW_RESULTS'
    CCVA_MODULE_RESULT_ERROR_DOWNLOAD: str= 'CCVA_MODULE_RESULT_ERROR_DOWNLOAD'
    
    
    


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
 