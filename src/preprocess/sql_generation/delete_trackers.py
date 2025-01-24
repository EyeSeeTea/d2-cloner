from src.preprocess.sql_generation.sql_common import write, convert_to_sql_format


def generate_delete_tracker_rules(trackers, data_elements, org_units, org_unit_descendants,
                                  all_uid, f):
    write(f, "--remove trackers \n")
    sql_all = convert_to_sql_format(all_uid)
    sql_trackers = convert_to_sql_format(trackers)
    sql_data_elements = convert_to_sql_format(data_elements)
    sql_org_units = convert_to_sql_format(org_units)
    sql_org_unit_descendants = convert_to_sql_format(org_unit_descendants)

    has_rule = False
    sql_query = "DELETE FROM programstageinstance where programstageid in " \
                " (select programstageid from programstage where programid in " \
                " (select programid from program where uid in {})) ".format(
        sql_all)
    if sql_data_elements != "":
        has_rule = True
        sql_data_elements = sql_data_elements.replace("(", "").replace(")", "")
        sql_query = " update programstageinstance set eventdatavalues = eventdatavalues - {}  where eventdatavalues ? " \
                    " {} and ".format(sql_data_elements, sql_data_elements)

        if sql_trackers != "":
            sql_query = sql_query + " programstageid in (select programstageid from programstage where programid in " \
                                    " (select programid from program where uid in {})) and".format(
                sql_trackers)
        else:
            sql_query = sql_query + " programstageid in (select programstageid from programstage where programid in " \
                                    " (select programid from program where uid in {})) and".format(
                sql_all)

    elif sql_trackers != "":
        has_rule = True
        sql_query = sql_query + " and dataelementid in (select dataelementid from datasetelement where datasetid in " \
                                " ( select datasetid from dataset where uid in {})) ".format(
            sql_trackers)
    if sql_org_units != "":
        has_rule = True
        sql_query = sql_query + "  and organisationunitid in (select organisationunitid from organisationunit where " \
                                " uid in {}) and".format(sql_org_units)
    if sql_org_unit_descendants != "":
        has_rule = True
        sql_query = sql_query + " and organisationunitid in (select organisationunitid from organisationunit where " \
                                " path like (select concat(path,'/%') from organisationunit where uid in {})) and".format(
            sql_org_unit_descendants)

    if not has_rule:
        delete_all_tracker_programs(all_uid, f)
    else:
        sql_query = sql_query + ";"
        sql_query = sql_query.replace("and;", ";")
        write(f, sql_query + "\n")


def delete_all_tracker_programs(trackers, f):
    trackers = convert_to_sql_format(trackers)
    write(f, """
create MATERIALIZED view tei_to_remove as select trackedentityinstanceid "teiid"
from programinstance where programid in (select programid from program where uid in {trackers});
""".format(trackers=trackers))
    write(f, """
--remove all tracker
DELETE FROM trackedentitydatavalueaudit where programstageinstanceid 
in ( select psi.programstageinstanceid  from programstageinstance psi 
inner join programstage ps on ps.programstageid=psi.programstageid 
inner join program p on p.programid=ps.programid 
where p.uid in {trackers});
""".format(trackers=trackers))
    write(f, """
DELETE FROM programstageinstancecomments where programstageinstanceid 
in ( select programstageinstanceid from programstageinstance where programstageid in 
(select programstageid from programstage where programid in 
(select programid from program where uid in {trackers})));
""".format(trackers=trackers))

    write(f, """
DELETE FROM trackedentityattributevalue where trackedentityinstanceid 
in ( select trackedentityinstanceid from programinstance where programid in(select 
programid from program where uid in {trackers}));
""".format(trackers=trackers))
    write(f, """
DELETE FROM trackedentityattributevalueaudit where trackedentityinstanceid 
in ( select trackedentityinstanceid from programinstance where programid 
in(select programid from program where uid in {trackers}));
""".format(trackers=trackers))

    write(f,
          """DELETE FROM programstageinstance where programstageid in (
          select programstageid from programstage where programid in 
          (select programid from program where uid in {trackers}));
          """.format(trackers=trackers))

    write(f, """
DELETE FROM programstageinstance where programstageinstanceid 
in ( select psi.programstageinstanceid  from programstageinstance psi 
inner join programstage ps on ps.programstageid=psi.programstageid 
inner join program p on p.programid=ps.programid 
where p.uid in {trackers});""".format(trackers=trackers))
    write(f,
          "DELETE FROM programinstancecomments where programinstanceid in (select programinstanceid from programinstance where programid in (select programid from program where uid in {trackers}));\n".format(
              trackers=trackers))
    write(f,
          "DELETE FROM programinstance where programid in (select programid from program where uid in {trackers});\n".format(
              trackers=trackers))
    write(f, """DELETE FROM programinstance where trackedentityinstanceid in ( select * from tei_to_remove);
DELETE FROM trackedentityinstance where trackedentityinstanceid in ( select * from tei_to_remove);
DELETE FROM trackedentityprogramowner where trackedentityinstanceid in ( select * from tei_to_remove);
drop view tei_to_remove ;
--remove tracker finish
""")
