from typing import NamedTuple

from src.common.config import Config
from src.preprocess.sql_generation.table_key import TableKey

TABLES_2_41 = {
    TableKey.EVENT: "event",
    TableKey.ENROLLMENT: "enrollment",
    TableKey.TRACKER: "trackedentity",
    TableKey.EVENT_COMMENT: "event_notes",
    TableKey.ENROLLMENT_COMMENT: "enrollment_notes",
    TableKey.USER: "userinfo"
}

TABLES_2_38 = {
    TableKey.EVENT: "programstageinstance",
    TableKey.ENROLLMENT: "programinstance",
    TableKey.TRACKER: "trackedentityinstance",
    TableKey.EVENT_COMMENT: "programstageinstancecomments",
    TableKey.ENROLLMENT_COMMENT: "programinstancecomments",
    TableKey.USER: "user"
}

def _get_tables_for_version():
    version = Config.get_pre_api_version()
    if version >= 41:
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


def get_interpretation_likedby_table() -> str:
    """Renamed from the misspelled intepretation_likedby in DHIS2 2.43 (V2_43_33)"""
    if Config.get_pre_api_version() >= 43:
        return "interpretation_likedby"
    return "intepretation_likedby"


def get_changelog_tables() -> list:
    """Change log tables with a foreign key (no ON DELETE CASCADE) to tracked entities or events.
    2.42 adds trackedentitychangelog (V2_42_31) and eventchangelog (V2_42_25);
    2.43 splits eventchangelog into trackereventchangelog and singleeventchangelog (V2_43_21)."""
    version = Config.get_pre_api_version()
    tables = []
    if version >= 42:
        tables.append("trackedentitychangelog")
    if version == 42:
        tables.append("eventchangelog")
    if version >= 43:
        tables.extend(["trackereventchangelog", "singleeventchangelog"])
    return tables


def get_orgunit_resource_tables() -> list:
    """Analytics resource tables with an organisationunitid column. Named _<name> until DHIS2 2.40 and
    analytics_rs_<name> from 2.41 (2.43 renames the old ones to _deprecated_<name>). They only exist once the
    resource tables have been generated, so callers must check that they exist."""
    prefix = "analytics_rs_" if Config.get_pre_api_version() >= 41 else "_"
    return [prefix + name for name in ("orgunitstructure", "datasetorganisationunitcategory",
                                       "organisationunitgroupsetstructure")]


def get_tracker_event_reference_column() -> str:
    """Column used by programmessage/programnotificationinstance to reference a tracker program event.
    Split into trackereventid/singleeventid from DHIS2 2.43 onward; a single eventid column before that."""
    if Config.get_pre_api_version() >= 43:
        return "trackereventid"
    return "eventid"


class EventTable(NamedTuple):
    """One physical table holding events. Before DHIS2 2.43 there is a single table; from 2.43 on
    events are split into trackerevent (with enrollment) and singleevent (without enrollment)."""
    table: str
    identifier: str
    comment_table: str
    # Column used by programmessage, programnotificationinstance and relationshipitem to reference this table
    reference_column: str
    has_enrollment: bool
    # Suffix for the temporary materialized views of this table (empty keeps the legacy view names)
    view_suffix: str


def get_event_tables() -> list:
    if Config.get_pre_api_version() >= 43:
        return [
            EventTable("trackerevent", "eventid", "trackerevent_notes", "trackereventid", True, "_trackerevent"),
            EventTable("singleevent", "eventid", "singleevent_notes", "singleeventid", False, "_singleevent"),
        ]
    return [EventTable(get_event_table_name(), get_event_identifier_name(), get_event_comment_table(),
                       get_event_identifier_name(), True, "")]
