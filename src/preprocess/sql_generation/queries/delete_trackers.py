from src.preprocess.sql_generation.sql_common import write, convert_to_sql_format, fix_final_query
from src.preprocess.sql_generation.versioned_table_names import *




def generate_delete_tracker_rules(trackers, data_elements, org_units, org_unit_descendants,
                                  all_uid, f):
    sql_all = convert_to_sql_format(all_uid)
    sql_trackers = convert_to_sql_format(trackers)
    tracker_program_uids_sql = sql_trackers if sql_trackers != "" else sql_all
    if tracker_program_uids_sql == "":
        write(f, f"""
        SELECT 'No UIDs provided for this department; skipping tracked entity and tracker program deletion' AS info;
        """)
        return


    write(f, f"""
    SELECT 'Starting DELETE block for trackerPrograms: All: '
           || quote_literal($${sql_all or 'NO_IDS'}$$)
           || ' and Detailed: '
           || quote_literal($${sql_trackers or 'NO_IDS'}$$) as info;
    """)
    sql_data_elements = convert_to_sql_format(data_elements)
    sql_org_units = convert_to_sql_format(org_units)
    sql_org_unit_descendants = convert_to_sql_format(org_unit_descendants)

    if sql_data_elements != "" or sql_org_units != "" or sql_org_unit_descendants != "":
        sql_query = compose_custom_query(tracker_program_uids_sql, sql_data_elements, sql_org_units, sql_org_unit_descendants)
        delete_mandatory_dependencies(f, tracker_program_uids_sql)
        write(f, fix_final_query(sql_query) + "\n")
    else:
        delete_all_tracker_programs(tracker_program_uids_sql, f, False)

def compose_custom_query(tracker_uis, sql_data_elements, sql_org_units, sql_org_unit_descendants):
    sql_data_elements = sql_data_elements.replace("(", "").replace(")", "")
    if sql_data_elements != "":
        # Removing these data elements requires updating the eventdatavalues JSONB.
        sql_query = """ 
                     update {programstageinstance} set eventdatavalues = eventdatavalues - {remove_data_elements_uid}  where eventdatavalues ? 
                     {where_data_elements_uid} and programstageid in (select programstageid from programstage where programid in
                     (select programid from program where uid in {tracker_program_uid})) and 
                 """.format(programstageinstance=get_event_table_name(), remove_data_elements_uid=sql_data_elements,
                            where_data_elements_uid=sql_data_elements, tracker_program_uid=tracker_uis)
    else:
        # If we don't need to remove data values, we should delete the events.
        sql_query = """
            DELETE FROM {programstageinstance} where programstageid in 
            (select programstageid from programstage where programid in 
            (select programid from program where uid in {uids})) 
        """.format(programstageinstance=get_event_table_name(), uids=tracker_uis)

    if sql_org_units != "":
        sql_query = sql_query + """ 
             and organisationunitid in (select organisationunitid from organisationunit where 
             uid in {org_units}) and 
         """.format(org_units=sql_org_units)

    if sql_org_unit_descendants != "":
        sql_query = sql_query + """ 
        and organisationunitid in (SELECT DISTINCT child.organisationunitid
            FROM organisationunit AS child
            JOIN organisationunit AS parent
            ON child.path LIKE parent.path || '/%'
            WHERE parent.uid IN {org_units}) and   
        """.format(org_units=sql_org_unit_descendants)
    return sql_query

def delete_mandatory_dependencies(f, trackers, exclude=False):
    """this query is called only when we are trying to remove only a custom dataelements/orgunits/orgunit leveles from tracker programs"""
    operator = "not in" if exclude else "in"
    write(f, """
        --remove all tracker dependencies
        DELETE FROM trackedentitydatavalueaudit where {programstageinstanceid}
        in ( select psi.{programstageinstanceid}  from {programstageinstance} psi
        inner join programstage ps on ps.programstageid=psi.programstageid
        inner join program p on p.programid=ps.programid
        where p.uid {operator} {tracker_uids});
    """.format(programstageinstanceid=get_event_identifier_name(), programstageinstance=get_event_table_name(),
               tracker_uids=trackers, operator=operator))
    write(f, """
        DELETE FROM {programstageinstancecomments} where {programstageinstanceid}
        in ( select {programstageinstanceid} from {programstageinstance} where programstageid in
        (select programstageid from programstage where programid in
        (select programid from program where uid {operator} {tracker_uids}))); 
    """.format(programstageinstancecomments=get_event_comment_table(),
               programstageinstanceid=get_event_identifier_name(), programstageinstance=get_event_table_name(),
               tracker_uids=trackers, operator=operator))

    write(f, f"""
    SELECT 'Close DELETE TRACKER PROGRAM Block' AS info;
    """)

def delete_all_tracker_programs_from_lists(trackers, f):
    tracker_uids_sql = convert_to_sql_format(trackers)
    delete_all_tracker_programs(tracker_uids_sql, f, False)

def delete_all_tracker_programs_not_in_lists(trackers, f):
    tracker_uids_sql = convert_to_sql_format(trackers)
    delete_all_tracker_programs(tracker_uids_sql, f, True)


def create_index_to_improve_deletion(f):
    write(f, """
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tei_to_remove_trid ON tei_to_remove({trackedentityid}); 
         CREATE INDEX IF NOT EXISTS idx_events_to_remove ON events_to_remove ({eventid});
        CREATE INDEX IF NOT EXISTS idx_enrollments_to_remove ON enrollments_to_remove ({enrollmentid});
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_programs_to_remove_pid ON programs_to_remove(programid);
    """.format(trackedentityid=get_tracker_identifier_name(), event=get_event_table_name(),
               eventid=get_event_identifier_name(),enrollmentid=get_enrollment_identifier_name()))


def delete_all_tracker_programs(trackers, f, exclude=False):
    write(f, f"""
        SELECT 'Starting DELETE block for trackerPrograms: '
               || quote_literal($${trackers or 'NO_IDS'}$$)
               || ' not in?: {exclude}' AS info;
        """)
    operator = "not in" if exclude else "in"

    if not trackers:
        #If the selected or unlisted tracker uids is empty, we must exit without remove trackers
        write(f, "SELECT 'No tracker UIDs provided; skipping delete block' AS info;\n")
        return

    #Careful: since the NOT IN operator is applied in the following queries, the initial one must be an IN for all cases.
    write(f, """
        DROP MATERIALIZED VIEW IF EXISTS tei_to_remove;
        create MATERIALIZED view tei_to_remove as select DISTINCT {trackedentityid} as "trackedentityid"
        from {enrollment} where programid in (select programid from program where uid {operator} {tracker_uids}) 
        and {trackedentityid} is not null ;
    """.format(trackedentityid=get_tracker_identifier_name(),
               enrollment=get_enrollment_table_name(),
               tracker_uids=trackers, operator=operator))
    write(f, """
        DROP MATERIALIZED VIEW IF EXISTS programs_to_remove;
        create MATERIALIZED view programs_to_remove as (select programid from program where uid {operator} {tracker_uids});
        
    """.format(tracker_uids=trackers, operator=operator))
    # Group all enrollments to be removed from the target programs, plus events in enrollments of tracked entities within those programs
    write(f, """
            DROP MATERIALIZED VIEW IF EXISTS enrollments_to_remove;
            CREATE MATERIALIZED VIEW enrollments_to_remove AS
                SELECT e.{enrollmentid}
                FROM {enrollment} e
                WHERE e.programid IN (SELECT programid FROM programs_to_remove)
                UNION
                SELECT e.{enrollmentid}
                FROM {enrollment} e
                WHERE e.{trackedentityid} IN (SELECT {trackedentityid} FROM tei_to_remove);
        """.format(enrollmentid=get_enrollment_identifier_name(), enrollment=get_enrollment_table_name(), trackedentityid=get_tracker_identifier_name()))
    # Group all events to be removed from the target programs, plus events in enrollments of tracked entities within those programs
    write(f, """
            DROP MATERIALIZED VIEW IF EXISTS events_to_remove;
            CREATE MATERIALIZED VIEW events_to_remove AS
                SELECT e.{eventid}
                FROM {event} e
                WHERE e.programstageid IN (
                    SELECT ps.programstageid
                    FROM programstage ps
                    WHERE ps.programid IN (SELECT programid FROM programs_to_remove)
                )
                UNION
                SELECT e.{eventid}
                FROM {event} e
                WHERE e.{enrollmentid} IN (
                    SELECT en.{enrollmentid}
                    FROM {enrollment} en
                    WHERE en.{trackedentityid} IN (SELECT trackedentityid FROM tei_to_remove)
                );
        """.format(
        event=get_event_table_name(), eventid=get_event_identifier_name(), enrollment=get_enrollment_table_name(),
        enrollmentid=get_enrollment_identifier_name(), trackedentityid=get_tracker_identifier_name()))
    create_index_to_improve_deletion(f)
    #Informative query
    write(f, """
      SELECT 'Events to be deleted: ' || COUNT(*)::text AS info FROM events_to_remove;
      SELECT 'Enrollments to be deleted: ' || COUNT(*)::text FROM enrollments_to_remove;
      SELECT 'Tracked entities to be deleted: ' || COUNT(*)::text FROM tei_to_remove;
    """)
    # Remove Tracked entity data value audits (audits from events) -  program and tei-enrollment-program

    write(f, "SELECT 'Deleting trackedentitydatavalueaudit....' AS info;\n")
    write(f, """
        DELETE FROM trackedentitydatavalueaudit 
        WHERE {eventid} IN (SELECT {eventid} FROM events_to_remove);
    """.format(
        event=get_event_table_name(),
        eventid=get_event_identifier_name(),
        enrollment=get_enrollment_table_name(),
        enrollmentid=get_enrollment_identifier_name(),
        trackedentityid=get_tracker_identifier_name()
    ))

    # Remove Event comments in program and tei-enrollment-program
    write(f, "SELECT 'Deleting event_comment....' AS info;\n")
    write(f, """
        DELETE FROM {event_comment} 
        WHERE {eventid} IN (SELECT {eventid} FROM events_to_remove);
    """.format(
        event_comment=get_event_comment_table(),
        eventid=get_event_identifier_name(),
        event=get_event_table_name(),
        enrollment=get_enrollment_table_name(),
        enrollmentid=get_enrollment_identifier_name(),
        trackedentityid=get_tracker_identifier_name()
    ))

    # Remove TrackedAttributeValueAudits in teis from programs
    write(f, "SELECT 'Deleting trackedentityattributevalueaudit....' AS info;\n")
    write(f, """
        DELETE FROM trackedentityattributevalueaudit
        WHERE {trackedentityid} IN (SELECT trackedentityid FROM tei_to_remove);
    """.format(
        trackedentityid=get_tracker_identifier_name(),
        enrollment=get_enrollment_table_name()
    ))
    # TrackedAttributeValues in teis from programs
    write(f, "SELECT 'Deleting trackedentityattributevalue....' AS info;\n")
    write(f, """
        DELETE FROM trackedentityattributevalue
        WHERE {trackedentityid} IN (SELECT trackedentityid FROM tei_to_remove);
    """.format(
        trackedentityid=get_tracker_identifier_name(),
        enrollment=get_enrollment_table_name()
    ))

    # Delete events from the selected programs and any events linked via enrollments of tracked entities marked for removal (even if they belong to other programs)
    write(f, "SELECT 'Deleting event....' AS info;\n")
    write(f, """
        DELETE FROM {event} e
        WHERE {eventid} IN (SELECT {eventid} FROM events_to_remove);
    """.format(
        event=get_event_table_name(), eventid = get_event_identifier_name()
    ))

    # Remove enrollment_comment in programs and teis.
    write(f, "SELECT 'Deleting enrollment_comment....' AS info;\n")
    write(f, """
        DELETE FROM {enrollment_comment}
        WHERE {enrollmentid} IN (select {enrollmentid} FROM enrollments_to_remove);
    """.format(
        enrollment_comment=get_enrollment_comment_table(),
        enrollmentid=get_enrollment_identifier_name(),
    ))

    # Remove enrollment in programs and teis.
    write(f, "SELECT 'Deleting enrollment....' AS info;\n")
    write(f, """
        DELETE FROM {enrollment} e
        WHERE {enrollmentid} IN (select {enrollmentid} FROM enrollments_to_remove);
    """.format(
        enrollment=get_enrollment_table_name(),
        enrollmentid=get_enrollment_identifier_name()
    ))

    # 9) Limpieza final de TEIs y relaciones
    write(f, "SELECT 'Deleting trackedentity and dependencies....' AS info;\n")
    write(f, """
        DELETE FROM trackedentityattributevalue WHERE {trackedentityid} IN (SELECT trackedentityid FROM tei_to_remove);
        DELETE FROM trackedentityprogramowner WHERE {trackedentityid} IN (SELECT trackedentityid FROM tei_to_remove);
        DELETE FROM {trackedentity} WHERE {trackedentityid} IN (SELECT trackedentityid FROM tei_to_remove);

        DROP MATERIALIZED VIEW IF EXISTS tei_to_remove;
        DROP MATERIALIZED VIEW IF EXISTS programs_to_remove;
        DROP MATERIALIZED VIEW IF EXISTS events_to_remove;
        DROP MATERIALIZED VIEW IF EXISTS enrollments_to_remove;
    """.format(
        trackedentity=get_tracker_table_name(),
        trackedentityid=get_tracker_identifier_name()
    ))

    write(f, "SELECT 'Close DELETE TRACKER PROGRAM Block' AS info;\n")