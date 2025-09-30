from src.preprocess.sql_generation.sql_common import write, convert_to_sql_format, fix_final_query
from src.preprocess.sql_generation.versioned_table_names import *




def generate_delete_tracker_rules(trackers, data_elements, org_units, org_unit_descendants,
                                  all_uid, f):
    sql_all = convert_to_sql_format(all_uid)
    sql_trackers = convert_to_sql_format(trackers)
    write(f, f"""
    SELECT 'Starting DELETE block for trackerPrograms: All: '
           || quote_literal($${sql_all or 'NO_IDS'}$$)
           || ' and Detailed: '
           || quote_literal($${sql_trackers or 'NO_IDS'}$$) as info;
    """)
    sql_data_elements = convert_to_sql_format(data_elements)
    sql_org_units = convert_to_sql_format(org_units)
    sql_org_unit_descendants = convert_to_sql_format(org_unit_descendants)
    has_rule = False
    sql_query = """
        DELETE FROM {programstageinstance} where programstageid in 
        (select programstageid from programstage where programid in 
        (select programid from program where uid in {uids})) 
    """.format(programstageinstance=get_event_table_name(),uids=sql_all)

    if sql_data_elements != "":
        has_rule = True
        sql_data_elements = sql_data_elements.replace("(", "").replace(")", "")
        sql_query = """ 
             update {programstageinstance} set eventdatavalues = eventdatavalues - {remove_data_elements_uid}  where eventdatavalues ? 
             {where_data_elements_uid} and 
         """.format(programstageinstance=get_event_table_name(),remove_data_elements_uid=sql_data_elements,
                    where_data_elements_uid=sql_data_elements)

        if sql_trackers != "":
            sql_query = sql_query + """ 
                programstageid in (select programstageid from programstage where programid in 
                (select programid from program where uid in {tracker_program_uid})) and 
            """.format(tracker_program_uid=sql_trackers)
        else:
            sql_query = sql_query + """ 
                 programstageid in (select programstageid from programstage where programid in 
                 (select programid from program where uid in {tracker_program_uid})) and 
             """.format(tracker_program_uid=sql_all)

    if sql_org_units != "":
        has_rule = True
        sql_query = sql_query + """ 
             and organisationunitid in (select organisationunitid from organisationunit where 
             uid in {org_units}) and 
         """.format(org_units=sql_org_units)

    if sql_org_unit_descendants != "":
        has_rule = True
        sql_query = sql_query + """ 
        and organisationunitid in (select organisationunitid from organisationunit where 
        path like (select concat(path,'/%') from organisationunit where uid in {org_units})) and 
        """.format(org_units=sql_org_unit_descendants)

    if not has_rule:
        delete_all_tracker_programs_from_lists(all_uid, f)
    else:
        delete_mandatory_dependencies(f, sql_trackers)
        write(f, fix_final_query(sql_query) + "\n")


def delete_mandatory_dependencies(f, trackers, exclude=False):
    """todo review all the dependencies"""
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
    trackers = convert_to_sql_format(trackers)
    delete_all_tracker_programs(trackers, f)

def delete_all_tracker_programs_not_in_lists(trackers, f):
    trackers = convert_to_sql_format(trackers)
    delete_all_tracker_programs(trackers, f, True)


def create_index_to_improve_deletion(f):
    write(f, """
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tei_to_remove_trid ON tei_to_remove(trackedentityid); 
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_programs_to_remove_pid ON programs_to_remove(programid);
    """)


def delete_all_tracker_programs(trackers, f, exclude=False):
    write(f, f"""
        SELECT 'Starting DELETE block for trackerPrograms: '
               || quote_literal($${trackers or 'NO_IDS'}$$)
               || ' not in?: {exclude}' AS info;
        """)
    operator = "not in" if exclude else "in"
    #Careful: since the NOT IN operator is applied in the following queries, the initial one must be an IN for all cases.
    write(f, """
        DROP MATERIALIZED VIEW IF EXISTS tei_to_remove;
        create MATERIALIZED view tei_to_remove as select DISTINCT {trackedentityinstanceid} as "trackedentityid"
        from {programinstance} where programid in (select programid from program where uid {operator} {tracker_uids}) 
        and {trackedentityinstanceid} is not null ;
    """.format(trackedentityinstanceid=get_tracker_identifier_name(),
               programinstance=get_enrollment_table_name(),
               tracker_uids=trackers, operator=operator))
    write(f, """
        DROP MATERIALIZED VIEW IF EXISTS programs_to_remove;
        create MATERIALIZED view programs_to_remove as (select programid from program where uid {operator} {tracker_uids});
    """.format(tracker_uids=trackers, operator=operator))
    create_index_to_improve_deletion(f)
    write(f, """
        DELETE FROM trackedentitydatavalueaudit where {programstageinstanceid}
        in ( select psi.{programstageinstanceid}  from {programstageinstance} psi
        inner join programstage ps on ps.programstageid=psi.programstageid
        where ps.programid in (select programid from programs_to_remove));
    """.format(programstageinstanceid=get_event_identifier_name(), programstageinstance=get_event_table_name()))

    write(f, """
        DELETE FROM {programstageinstancecomments} where {programstageinstanceid}
        in (select {programstageinstanceid} from {programstageinstance} where programstageid in
        (select programstageid from programstage where programid in (select programid from programs_to_remove)
         )); 
    """.format(programstageinstancecomments=get_event_comment_table(),
               programstageinstanceid=get_event_identifier_name(), programstageinstance=get_event_table_name()))

    write(f, """
        DELETE FROM trackedentityattributevalueaudit where {trackedentityinstanceid}
        in ( select {trackedentityinstanceid} from {programinstance} where programid in (select programid from programs_to_remove));
    """.format(trackedentityinstanceid=get_tracker_identifier_name(), programinstance=get_enrollment_table_name()))

    write(f, """
        DELETE FROM trackedentityattributevalue where {trackedentityinstanceid}
        in ( select {trackedentityinstanceid} from {programinstance} where programid in (select programid from programs_to_remove));
    """.format(trackedentityinstanceid=get_tracker_identifier_name(), programinstance=get_enrollment_table_name()))

    write(f,"""
        DELETE FROM {programstageinstance} where programstageid in
        (select programstageid from programstage where 
        programid in (select programid from programs_to_remove));
    """.format(programstageinstance=get_event_table_name()))

    write(f, """
        DELETE FROM {programstageinstance} where {programstageinstanceid}
        in ( select psi.{programstageinstanceid}  from {programstageinstance} psi
        inner join programstage ps on ps.programstageid=psi.programstageid
        where ps.programid in (select programid from programs_to_remove));
    """.format(programstageinstance=get_event_table_name(),
               programstageinstanceid=get_event_identifier_name()))

    write(f,"""
        DELETE FROM {programstageinstance} where {programinstanceid} in (select {programinstanceid} from {programinstance}  
        where programstageid in (select programstageid from programstage where programid in (select programid from programs_to_remove)));\n
    """.format(programstageinstance=get_event_table_name(), programinstanceid= get_enrollment_identifier_name(),
               programinstance=get_enrollment_table_name()))


    write(f,"""
            DELETE FROM {programinstancecomments} 
            where {programinstanceid} in (select {programinstanceid} from {programinstance}
            where programid in (select programid from programs_to_remove));\n
          """.format(programinstancecomments=get_enrollment_comment_table(),
                     programinstanceid=get_enrollment_identifier_name(),
                     programinstance=get_enrollment_table_name()))
    write(f,"""
        DELETE FROM {programinstance}
        where programid in (select programid from programs_to_remove);\n
    """.format(programinstance=get_enrollment_table_name()))

    write(f, """
        DELETE from trackedentitydatavalueaudit where {programstageinstanceid} in
        (select {programstageinstanceid} from {programstageinstance} where {programinstanceid} 
        in (select {programinstanceid} FROM {programinstance} where {trackedentityinstanceid} in ( select trackedentityid from tei_to_remove) 
        and programid in (select programid from programs_to_remove)));"""
          .format(programstageinstance=get_event_table_name(), programstageinstanceid=get_event_identifier_name(),
                  programinstanceid=get_enrollment_identifier_name(), programinstance=get_enrollment_table_name(),
               trackedentityinstanceid=get_tracker_identifier_name()))

    write(f, """
            DELETE from {programstageinstance} where {programinstanceid} in (
            select {programinstanceid} FROM {programinstance} where {trackedentityinstanceid} in ( select trackedentityid from tei_to_remove)
            AND programstageid IN (
              SELECT programstageid FROM programstage
              WHERE programid IN (SELECT programid FROM programs_to_remove)
            ))
            ;"""
              .format(programstageinstance=get_event_table_name(), programinstanceid=get_enrollment_identifier_name(),
                      programinstance=get_enrollment_table_name(),
                      trackedentityinstanceid=get_tracker_identifier_name()))

    write(f, """
            DELETE FROM {programinstance} where {trackedentityinstanceid} in ( select trackedentityid from tei_to_remove) and programid in  (select programid from programs_to_remove);
        """.format(programinstance=get_enrollment_table_name(), trackedentityinstanceid=get_tracker_identifier_name()))

    write(f, """
            DELETE FROM trackedentityattributevalue where {trackedentityinstanceid} in ( select trackedentityid from tei_to_remove);
            DELETE FROM trackedentityprogramowner where {trackedentityinstanceid} in ( select trackedentityid from tei_to_remove);
            DELETE FROM {trackedentityinstance} where {trackedentityinstanceid} in ( select trackedentityid from tei_to_remove);
            DROP MATERIALIZED VIEW IF EXISTS tei_to_remove;
            DROP MATERIALIZED VIEW IF EXISTS programs_to_remove;
        """.format(trackedentityinstance=get_tracker_table_name(),
                    trackedentityinstanceid=get_tracker_identifier_name()))
    write(f, f"""
    SELECT 'Close DELETE TRACKER PROGRAM Block' AS info;
    """)
