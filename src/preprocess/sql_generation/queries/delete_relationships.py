from src.common.config import Config
from src.preprocess.sql_generation.sql_common import write

# From DHIS2 2.39 (V2_39_8) relationship and relationshipitem cascade on delete in both directions, so deleting one
# item also deletes its relationship and the relationship's other item. Older versions are left untouched.
MIN_VERSION_WITH_RELATIONSHIP_CASCADE = 39


def delete_relationship_items(f, references):
    """Delete the relationship items that point to rows about to be deleted: relationshipitem has foreign keys
    without cascade to the tracked entity, enrollment and event tables, so it would block their deletion.
    references: list of (relationshipitem column, SQL query returning the ids to be deleted)"""
    if Config.get_pre_api_version() < MIN_VERSION_WITH_RELATIONSHIP_CASCADE:
        return
    write(f, "SELECT 'Deleting relationshipitem....' AS D2_DOCKER_PRESQL_SCRIPT;\n")
    for column, ids_query in references:
        write(f, """
        DELETE FROM relationshipitem WHERE {column} IN ({ids_query});
    """.format(column=column, ids_query=ids_query))


def custom_event_filter(program_uids, sql_org_units, sql_org_unit_descendants):
    """WHERE condition matching the events removed by the custom (org unit filtered) event deletion"""
    conditions = ["""programstageid IN (SELECT programstageid FROM programstage WHERE programid IN
            (SELECT programid FROM program WHERE uid IN {}))""".format(program_uids)]
    if sql_org_units != "":
        conditions.append("""organisationunitid IN (SELECT organisationunitid FROM organisationunit
            WHERE uid IN {})""".format(sql_org_units))
    if sql_org_unit_descendants != "":
        conditions.append("""organisationunitid IN (SELECT DISTINCT child.organisationunitid
            FROM organisationunit AS child
            JOIN organisationunit AS parent ON child.path LIKE parent.path || '/%'
            WHERE parent.uid IN {})""".format(sql_org_unit_descendants))
    return " AND ".join(conditions)


def delete_relationship_items_for_custom_event_deletion(f, event_table, program_uids, sql_org_units,
                                                        sql_org_unit_descendants):
    delete_relationship_items(f, [(event_table.reference_column, "SELECT {eventid} FROM {event} WHERE {condition}".format(
        eventid=event_table.identifier, event=event_table.table,
        condition=custom_event_filter(program_uids, sql_org_units, sql_org_unit_descendants)))])
