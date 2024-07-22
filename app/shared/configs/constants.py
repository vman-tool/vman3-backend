from enum import Enum


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