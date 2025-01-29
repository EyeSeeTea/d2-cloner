from src.preprocess.sql_generation.sql_common import write, convert_to_sql_format, fix_final_query
from src.preprocess.sql_generation.versioned_table_names import *


def generate_delete_tracker_rules(trackers, data_elements, org_units, org_unit_descendants,
                                  all_uid, f):
    write(f, "--remove trackers \n")
    sql_all = convert_to_sql_format(all_uid)
    sql_trackers = convert_to_sql_format(trackers)
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

    elif sql_trackers != "":
        has_rule = True
        sql_query = sql_query + """ 
            and dataelementid IN (
            SELECT DISTINCT dataelementid 
            FROM programstagedataelement psde
            INNER JOIN programstage ps ON psde.programstageid = ps.programstageid
            INNER JOIN program p ON ps.programid = p.programid
            WHERE p.uid IN {progam_uids})
         """.format(progam_uids=sql_trackers)

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
        delete_all_tracker_programs(all_uid, f)
    else:
        write(f, fix_final_query(sql_query) + "\n")


def delete_all_tracker_programs(trackers, f):
    trackers = convert_to_sql_format(trackers)
    write(f, """
        create MATERIALIZED view tei_to_remove as select {trackedentityinstanceid} "teiid"
        from {programinstance} where programid in (select programid from program where uid in {tracker_uids});
    """.format(trackedentityinstanceid=get_enrollment_identifier_name(),
               programinstance=get_enrollment_table_name(),
               tracker_uids=trackers))
    write(f, """
        --remove all tracker
        DELETE FROM trackedentitydatavalueaudit where {programstageinstanceid}
        in ( select psi.{programstageinstanceid}  from {programstageinstance} psi
        inner join programstage ps on ps.programstageid=psi.programstageid
        inner join program p on p.programid=ps.programid
        where p.uid in {tracker_uids});
    """.format(programstageinstanceid=get_event_identifier_name(), programstageinstance=get_event_table_name(),
               tracker_uids=trackers))
    write(f, """
        DELETE FROM {programstageinstancecomments} where {programstageinstanceid}
        in ( select {programstageinstanceid} from {programstageinstance} where programstageid in
        (select programstageid from programstage where programid in
        (select programid from program where uid in {tracker_uids})));
    """.format(programstageinstancecomments=get_event_comment_table(),
               programstageinstanceid=get_event_identifier_name(), programstageinstance=get_event_table_name(),
               tracker_uids=trackers))

    write(f, """
        DELETE FROM trackedentityattributevalue where {trackedentityinstanceid} 
        in ( select {trackedentityinstanceid} from {programinstance} where programid in(select 
        programid from program where uid in {tracker_uids}));
    """.format(trackedentityinstanceid=get_tracker_identifier_name(), programinstance=get_enrollment_table_name(),
               tracker_uids=trackers))
    write(f, """
        DELETE FROM trackedentityattributevalueaudit where {trackedentityinstanceid} 
        in ( select {trackedentityinstanceid} from {programinstance} where programid 
        in(select programid from program where uid in {tracker_uids}));
    """.format(trackedentityinstanceid=get_tracker_identifier_name(), programinstance=get_enrollment_table_name(),
               tracker_uids=trackers))

    write(f,"""
        DELETE FROM {programstageinstance} where programstageid in 
        (select programstageid from programstage where programid in 
        (select programid from program where uid in {tracker_uids}));
    """.format(programstageinstance=get_event_table_name(), tracker_uids=trackers))

    write(f, """
        DELETE FROM {programstageinstance} where {programstageinstanceid}  
        in ( select psi.{programstageinstanceid}  from {programstageinstance} psi  
        inner join programstage ps on ps.programstageid=psi.programstageid 
        inner join program p on p.programid=ps.programid 
        where p.uid in {tracker_uids});
    """.format(programstageinstance=get_event_table_name(),
                                              programstageinstanceid=get_event_identifier_name(),
                                              tracker_uids=trackers))
    write(f,"""
            DELETE FROM {programinstancecomments} 
            where {programinstanceid} in (select {programinstanceid} from {programinstance} 
            where programid in (select programid from program where uid in 
            {tracker_uids}));\n
          """.format(programinstancecomments=get_enrollment_comment_table(),
                                        programinstanceid=get_enrollment_identifier_name(),
                                        programinstance=get_enrollment_table_name(),
                                        tracker_uids=trackers))
    write(f,"""
        DELETE FROM {programinstance} 
        where programid in (select programid from program where uid in {tracker_uids});\n
    """.format(programinstance=get_enrollment_table_name(),tracker_uids=trackers))

    write(f, """
        DELETE FROM {programinstance} where {trackedentityinstanceid} in ( select * from tei_to_remove);
        DELETE FROM {trackedentityinstance} where {trackedentityinstanceid} in ( select * from tei_to_remove);
        DELETE FROM trackedentityprogramowner where {trackedentityinstanceid} in ( select * from tei_to_remove);
        drop MATERIALIZED tei_to_remove ;
        --remove tracker finish
    """.format(programinstance=get_enrollment_table_name(), trackedentityinstance=get_tracker_table_name(),
               trackedentityinstanceid=get_tracker_identifier_name()))
