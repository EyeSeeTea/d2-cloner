from src.common.config import Config
from src.preprocess.sql_generation.table_key import TableKey

TABLES_2_41 = {
    TableKey.EVENT: "event",
    TableKey.ENROLLMENT: "enrollment",
    TableKey.TRACKER: "trackerentity",
    TableKey.EVENT_COMMENT: "event_notes",
    TableKey.ENROLLMENT_COMMENT: "enrollment_note",
    TableKey.USER: "userinfo"
}

TABLES_2_38 = {
    TableKey.EVENT: "programstageinstance",
    TableKey.ENROLLMENT: "programinstance",
    TableKey.TRACKER: "trackerentityinstance",
    TableKey.EVENT_COMMENT: "programstageinstancecomments",
    TableKey.ENROLLMENT_COMMENT: "programinstancecomments",
    TableKey.USER: "user"
}

def _get_tables_for_version():
    version = Config.get_instance().post_api_version
    if version >= 2.41:
        return TABLES_2_41
    else:
        return TABLES_2_38


def _get_table_name(key: TableKey) -> str:
    tables = _get_tables_for_version()
    return tables[key]


def _get_identifier_name(key: TableKey) -> str:
    return _get_table_name(key) + "id"


def get_event_table_name() -> str:
    return _get_table_name(TableKey.EVENT)

def get_event_identifier_name() -> str:
    return _get_identifier_name(TableKey.EVENT)

def get_enrollment_table_name() -> str:
    return _get_table_name(TableKey.ENROLLMENT)

def get_enrollment_identifier_name() -> str:
    return _get_identifier_name(TableKey.ENROLLMENT)

def get_tracker_table_name() -> str:
    return _get_table_name(TableKey.TRACKER)

def get_tracker_identifier_name() -> str:
    return _get_identifier_name(TableKey.TRACKER)

def get_event_comment_table() -> str:
    return _get_table_name(TableKey.EVENT_COMMENT)

def get_enrollment_comment_table() -> str:
    return _get_table_name(TableKey.ENROLLMENT_COMMENT)

def get_user_table_name() -> str:
    return _get_table_name(TableKey.USER)

def get_user_identifier_name() -> str:
    return _get_identifier_name(TableKey.USER)
