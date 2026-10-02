from src.preprocess.sql_generation.sql_common import write, convert_to_sql_format, fix_final_query
from src.preprocess.sql_generation.versioned_table_names import *

def generate_delete_event_rules(event_program, data_elements, org_units,
                                org_unit_descendants, all_uid, f):
    sql_all = convert_to_sql_format(all_uid)
    sql_event_program = convert_to_sql_format(event_program)
    sql_data_elements = convert_to_sql_format(data_elements)
    sql_org_units = convert_to_sql_format(org_units)
    sql_org_unit_descendants = convert_to_sql_format(org_unit_descendants)

    # Use the detailed event-program list if available; otherwise fall back to the 'all' UIDs.
    event_program_uids_sql = sql_event_program if sql_event_program != "" else sql_all
    if event_program_uids_sql == "":
        write(f, f"""
        SELECT 'No UIDs provided for this department; skipping event program deletion' AS D2_DOCKER_PRESQL_SCRIPT;
        """)
        return
    write(f, f"""
    SELECT 'Starting DELETE block  for eventPrograms Detailed: ' || quote_literal($${event_program_uids_sql or 'NO_IDS'}$$) AS D2_DOCKER_PRESQL_SCRIPT;
    \n
""")
    if sql_data_elements != "" or sql_org_units != "" or sql_org_unit_descendants != "":
        write(f, f"""
            SELECT
                'Starting CUSTOM QUERY block for eventPrograms: '
                || quote_literal($${event_program_uids_sql or 'NO_IDS'}$$)
                || ' | dataelements: ' || quote_literal($${sql_data_elements or 'NO_IDS'}$$)
                || ' | org_units: ' || quote_literal($${sql_org_units or 'NO_IDS'}$$)
                || ' | org_unit_descendants: ' || quote_literal($${sql_org_unit_descendants or 'NO_IDS'}$$)
            AS "D2_DOCKER_PRESQL_SCRIPT";
        """)
        for event_table in get_event_tables():
            sql_query = compose_custom_query(event_table, event_program_uids_sql, sql_data_elements, sql_org_units,
                                             sql_org_unit_descendants)
            write(f, fix_final_query(sql_query) + "\n")
    else:
        delete_all_event_programs(event_program_uids_sql, f)

    write(f, f"""
    SELECT 'Close DELETE EVENT PROGRAMS Block' AS D2_DOCKER_PRESQL_SCRIPT;
    """)


def compose_custom_query(event_table, event_program_uids, sql_data_elements, sql_org_units, sql_org_unit_descendants):
    sql_query = """ 
                    DELETE FROM {event} where programstageid in 
                    (select programstageid from programstage where programid in 
                    (select programid from program where uid in {uids})) and 
                 """.format(event=event_table.table,
                            uids=event_program_uids)
    if sql_data_elements != "":
        sql_data_elements = sql_data_elements.replace("(", "").replace(")", "")
        sql_query = """ 
                update {event} set eventdatavalues = eventdatavalues - {dataelements_to_remove} 
                where eventdatavalues ? {dataelement_filtered} and 
             """.format(
            event=event_table.table,
            dataelements_to_remove=sql_data_elements,
            dataelement_filtered=sql_data_elements)

        sql_query = sql_query + """
                     programstageid in (select programstageid from programstage
                     where programid in (select programid from program where uid in {uids})) and 
            """.format(uids=event_program_uids)

    if sql_org_units != "":
        sql_query = sql_query + """
                 organisationunitid in (select organisationunitid from organisationunit 
                 where uid in {uids}) and 
             """.format(uids=sql_org_units)

    if sql_org_unit_descendants != "":
        sql_query = sql_query + """
                 organisationunitid in (SELECT DISTINCT child.organisationunitid
                    FROM organisationunit AS child
                    JOIN organisationunit AS parent
                      ON child.path LIKE parent.path || '/%'
                        WHERE parent.uid IN {uids}) and  
              """.format(uids=sql_org_unit_descendants)
    return sql_query


def delete_all_event_programs_from_lists(programs, f):
    programs_uids_sql = convert_to_sql_format(programs)
    delete_all_event_programs(programs_uids_sql, f)

def create_index_to_improve_deletion(f, event_table):
    write(f, """
        CREATE INDEX IF NOT EXISTS idx_events_to_remove{suffix} ON events_to_remove{suffix} ({eventid});
        ANALYZE events_to_remove{suffix};
    """.format(eventid=event_table.identifier, suffix=event_table.view_suffix))

def delete_all_event_programs(programs, f):
    write(f, f"""
    SELECT 'Starting DELETE block for eventPrograms All: '
           || quote_literal($${programs or 'NO_IDS'}$$) AS D2_DOCKER_PRESQL_SCRIPT;
    """)
    for event_table in get_event_tables():
        delete_all_events_from_table(event_table, programs, f)
    write(f, f"""
    SELECT 'Close DELETE EVENT PROGRAMS Block' AS D2_DOCKER_PRESQL_SCRIPT;
    """)

def delete_all_events_from_table(event_table, programs, f):
    params = dict(event=event_table.table, eventid=event_table.identifier, programs=programs,
                  event_comment=event_table.comment_table, suffix=event_table.view_suffix)
    write(f, """
            DROP MATERIALIZED VIEW IF EXISTS events_to_remove{suffix};
            CREATE MATERIALIZED VIEW events_to_remove{suffix} AS
            select e.{eventid}  from {event} e
            where e.programstageid in 
                (SELECT programstageid FROM programstage where programid in 
                    (SELECT programid FROM program where uid in {programs})
                );
        """.format(**params))
    create_index_to_improve_deletion(f, event_table)
    write(f, """
      SELECT 'Events to be deleted from {event}: ' || COUNT(*)::text AS D2_DOCKER_PRESQL_SCRIPT FROM events_to_remove{suffix};
    """.format(**params))
    #remove event audit values
    write(f, "SELECT 'Deleting trackedentitydatavalueaudit....' AS D2_DOCKER_PRESQL_SCRIPT;\n")
    write(f, """
        DELETE FROM trackedentitydatavalueaudit 
        WHERE {eventid} IN (SELECT {eventid} FROM events_to_remove{suffix});
    """.format(**params))
    # Remove event_comments in programs
    write(f, "SELECT 'Deleting event_comment....' AS D2_DOCKER_PRESQL_SCRIPT;\n")
    write(f, """
        DELETE FROM {event_comment} 
        WHERE {eventid} IN (SELECT {eventid} FROM events_to_remove{suffix});
        """.format(**params))
    # Remove events in programs
    write(f, "SELECT 'Deleting events....' AS D2_DOCKER_PRESQL_SCRIPT;\n")
    write(f, """
        DELETE FROM {event} 
        WHERE {eventid} IN (SELECT {eventid} FROM events_to_remove{suffix});
        --end remove events block
    """.format(**params))
    write(f, """
        DROP MATERIALIZED VIEW IF EXISTS events_to_remove{suffix};
    """.format(**params))
