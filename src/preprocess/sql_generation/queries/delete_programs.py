from src.preprocess.sql_generation.sql_common import write, convert_to_sql_format, fix_final_query
from src.preprocess.sql_generation.versioned_table_names import *


def generate_delete_event_rules(event_program, data_elements, org_units,
                                org_unit_descendants, all_uid, f):
    sql_all = convert_to_sql_format(all_uid)
    sql_event_program = convert_to_sql_format(event_program)
    sql_data_elements = convert_to_sql_format(data_elements)
    sql_org_units = convert_to_sql_format(org_units)
    sql_org_unit_descendants = convert_to_sql_format(org_unit_descendants)
    write(f, f"""
    SELECT 'Starting DELETE block  for eventPrograms Detailed: ' || quote_literal($${sql_event_program or 'NO_IDS'}$$) AS info;
    \n
""")
    has_rule = False
    if sql_event_program != "":
        has_rule = True
        sql_query = """ 
            DELETE FROM {programstageinstance} where programstageid in 
            (select programstageid from programstage where programid in 
            (select programid from program where uid in {uids})) and 
         """.format(programstageinstance=get_event_table_name(),
                    uids=sql_event_program)
    else:
        sql_query = """ 
            DELETE FROM {programstageinstance} where programstageid in 
            (select programstageid from programstage where programid in 
            (select programid from program where uid in {uids})) and 
        """.format(
            programstageinstance=get_event_table_name(),
            uids=sql_all)
    if sql_data_elements != "":
        has_rule = True
        sql_data_elements = sql_data_elements.replace("(", "").replace(")", "")
        sql_query = """ 
            update {programstageinstance} set eventdatavalues = eventdatavalues - {dataelements_to_remove} 
            where eventdatavalues ? {dataelement_filtered} and 
         """.format(
            programstageinstance=get_event_table_name(),
            dataelements_to_remove=sql_data_elements,
            dataelement_filtered=sql_data_elements)
        if sql_event_program != "":
            sql_query = sql_query + """
                 programstageid in (select programstageid from programstage
                 where programid in (select programid from program where uid in {uids})) and 
             """.format(uids=sql_event_program)
        else:
            sql_query = sql_query + """ 
                programstageid in (select programstageid from programstage  
                where programid in (select programid from program where uid in {uids})) and 
            """.format(uids=sql_all)

    if sql_org_units != "":
        has_rule = True
        sql_query = sql_query + """
             organisationunitid in (select organisationunitid from organisationunit 
             where uid in {uids}) and 
         """.format(uids=sql_org_units)
    if sql_org_unit_descendants != "":
        has_rule = True
        sql_query = sql_query + """
             organisationunitid in (select organisationunitid from organisationunit  
             where path like (select concat(path,'/%') from organisationunit  
             where uid in {uids})) and 
          """.format(uids=sql_org_unit_descendants)

    if not has_rule:
        delete_all_event_programs_from_lists(all_uid, f)
    else:
        write(f, fix_final_query(sql_query) + "\n")

    write(f, f"""
    SELECT 'Close DELETE EVENT PROGRAMS Block' AS info;
    """)


def delete_all_event_programs_from_lists(programs, f):
    programs = convert_to_sql_format(programs)
    delete_all_event_programs(programs, f)

def delete_all_event_programs(programs, f):
    write(f, f"""
    SELECT 'Starting DELETE block for eventPrograms All: '
           || quote_literal($${programs or 'NO_IDS'}$$) AS info;
    """)

    write(f, """
        DELETE FROM trackedentitydatavalueaudit where {programstageinstanceid}
        in ( select psi.{programstageinstanceid}  from {programstageinstance} psi
        inner join programstage ps on ps.programstageid=psi.programstageid
        inner join program p on p.programid=ps.programid where p.uid in {programs});
    """.format(programstageinstanceid=get_event_identifier_name(),
               programstageinstance=get_event_table_name(),
               programs=programs))
    write(f,
          """
        DELETE FROM {programstageinstancecomments} where {programstageinstanceid}
        in ( select psi.{programstageinstanceid}  from {programstageinstance} psi
        inner join programstage ps on ps.programstageid=psi.programstageid
        inner join program p on p.programid=ps.programid where p.uid in {programs});
        """.format(programstageinstancecomments=get_event_comment_table(),
                   programstageinstanceid=get_event_identifier_name(),
                   programstageinstance=get_event_table_name(),
                   programs=programs))
    write(f, """
        DELETE FROM {programstageinstance} where {programstageinstanceid}
        in ( select psi.{programstageinstanceid}  from {programstageinstance} psi
        inner join programstage ps on ps.programstageid=psi.programstageid
        inner join program p on p.programid=ps.programid
        where p.uid in {programs});
        --end remove events block
    """.format(programstageinstanceid=get_event_identifier_name(),
           programstageinstance=get_event_table_name(), programs=programs))

    write(f, f"""
    SELECT 'Close DELETE EVENT PROGRAMS Block' AS info;
    """)