import json


def remove_all_unnecessary_dependencies(f):
    f.write("delete from programstageinstance_messageconversation;" + "\n")
    f.write("delete from programinstancecomments;" + "\n")
    f.write("delete from programinstanceaudit;" + "\n")
    f.write("delete from datavalueaudit;" + "\n")
    f.write("delete from trackedentitydatavalueaudit;" + "\n")
    f.write("delete from trackedentityattributevalueaudit;" + "\n")
    f.write("delete from programstageinstance_messageconversation;" + "\n")
    f.write("delete from dataapprovalaudit;" + "\n")
    f.write("delete from interpretation_comments;" + "\n")
    f.write("delete from interpretationcomment;" + "\n")
    f.write("delete from messageconversation_messages;" + "\n")
    f.write("delete from messageconversation_usermessages;" + "\n")
    f.write("delete from messageconversation;" + "\n")


def generate_delete_event_rules(event_program, data_elements, org_units, org_unit_descendants, all_uid, f):
    f.write("--remove events")
    sql_all = convert_to_sql_format(all_uid)
    sql_event_program = convert_to_sql_format(event_program)
    sql_data_elements = convert_to_sql_format(data_elements)
    sql_org_units = convert_to_sql_format(org_units)
    sql_org_unit_descendants = convert_to_sql_format(org_unit_descendants)
    has_rule = False
    if sql_event_program != "":
        has_rule = True
        sql_query = "delete from programstageinstance where programstageid in (select programstageid from programstage where programid in (select programid from program where uid in " + event_program + ")) and"
    else:
        sql_query = "delete from programstageinstance where programstageid in (select programstageid from programstage where programid in (select programid from program where uid in " + sql_all + ")) and"
    if sql_data_elements != "":
        has_rule = True
        sql_data_elements = sql_data_elements.replace("(", "").replace(")", "")
        sql_query = "update programstageinstance "
        sql_query = sql_query + " set eventdatavalues = eventdatavalues - " + sql_data_elements + "  "
        sql_query = sql_query + " where eventdatavalues ? " + sql_data_elements + " and"
        if sql_event_program != "":
            sql_query = sql_query + " programstageid in (select programstageid from programstage where programid in (select programid from program where uid in " + event_program + ")) and"
        else:
            sql_query = sql_query + " programstageid in (select programstageid from programstage where programid in (select programid from program where uid in " + sql_all + ")) and"
    if sql_org_units != "":
        has_rule = True
        sql_query = sql_query + " organisationunitid in (select organisationunitid from organisationunit where uid in " + sql_org_units + ") and"
    if sql_org_unit_descendants != "":
        has_rule = True
        sql_query = sql_query + " organisationunitid in (select organisationunitid from organisationunit where path like (select concat(path,'/%') from organisationunit where uid in " + sql_org_unit_descendants + ")) and"

    if not has_rule:
        delete_all_event_programs(all_uid, f)
    else:
        sql_query = sql_query + ";"
        sql_query = sql_query.replace("and;", ";")
        f.write(sql_query + "\n")


def generate_delete_tracker_rules(trackers, data_elements, org_units, org_unit_descendants, all_uid, f):
    f.write("--remove trackers")
    sql_all = convert_to_sql_format(all_uid)
    sql_trackers = convert_to_sql_format(trackers)
    sql_data_elements = convert_to_sql_format(data_elements)
    sql_org_units = convert_to_sql_format(org_units)
    sql_org_unit_descendants = convert_to_sql_format(org_unit_descendants)

    has_rule = False
    sql_query = "delete from programstageinstance where programstageid in (select programstageid from programstage where programid in (select programid from program where uid in " + sql_all + ")) "
    if sql_data_elements != "":
        has_rule = True
        sql_data_elements = sql_data_elements.replace("(", "").replace(")", "")
        sql_query = "update programstageinstance "
        sql_query = sql_query + " set eventdatavalues = eventdatavalues - " + sql_data_elements + "  "
        sql_query = sql_query + " where eventdatavalues ? " + sql_data_elements + " and"

        if sql_trackers != "":
            sql_query = sql_query + " programstageid in (select programstageid from programstage where programid in (select programid from program where uid in " + sql_trackers + ")) and"
        else:
            sql_query = sql_query + " programstageid in (select programstageid from programstage where programid in (select programid from program where uid in " + sql_all + ")) and"

    elif sql_trackers != "":
        has_rule = True
        sql_query = sql_query + " and dataelementid in (select dataelementid from datasetelement where datasetid in ( select datasetid from dataset where uid in " + sql_trackers + ")) "
    if sql_org_units != "":
        has_rule = True
        sql_query = sql_query + " and organisationunitid in (select organisationunitid from organisationunit where uid in " + sql_org_units + ") and"
    if sql_org_unit_descendants != "":
        has_rule = True
        sql_query = sql_query + " and organisationunitid in (select organisationunitid from organisationunit where path like (select concat(path,'/%') from organisationunit where uid in " + sql_org_unit_descendants + ")) and"

    if not has_rule:
        delete_all_tracker_programs(all_uid, f)
    else:
        sql_query = sql_query + ";"
        sql_query = sql_query.replace("and;", ";")
        f.write(sql_query + "\n")


def generate_delete_datasets_rules(datasets, data_elements, org_units, org_unit_descendants, all_uid, f):
    f.write("--remove datasets" + "\n")
    sql_all = convert_to_sql_format(all_uid)
    sql_datasets = convert_to_sql_format(datasets)
    sql_data_elements = convert_to_sql_format(data_elements)
    sql_org_units = convert_to_sql_format(org_units)
    sql_org_unit_descendants = convert_to_sql_format(org_unit_descendants)
    sql_query = "delete from datavalue where dataelementid in (select dataelementid from datasetelement where datasetid in ( select datasetid from dataset where uid in " + sql_all + ")) and"
    has_rule = False
    if sql_data_elements != "":
        has_rule = True
        if sql_datasets != "":
            sql_query = sql_query + " dataelementid in (select dataelementid from dataelement where uid in " + sql_data_elements + " and dataelementid in (select dataelementid from datasetelement where datasetid in ( select datasetid from dataset where uid in " + sql_datasets + "))) and"
        else:
            sql_query = sql_query + " dataelementid in (select dataelementid from dataelement where uid in " + sql_data_elements + " and dataelementid in (select dataelementid from datasetelement where datasetid in ( select datasetid from dataset where uid in " + sql_all + "))) and"
    elif sql_datasets != "":
        has_rule = True
        sql_query = sql_query + " dataelementid in (select dataelementid from datasetelement where datasetid in ( select datasetid from dataset where uid in " + sql_datasets + ")) and"
    if sql_org_units != "":
        has_rule = True
        sql_query = sql_query + " sourceid in (select organisationunitid from organisationunit where uid in " + sql_org_units + ") and"
    if sql_org_unit_descendants != "":
        has_rule = True
        sql_query = sql_query + " sourceid in (select organisationunitid from organisationunit where path like (select concat(path,'/%') from organisationunit where uid in " + sql_org_unit_descendants + ")) and"

    if not has_rule:
        delete_all_data_sets(all_uid, f)
    else:
        sql_query = sql_query + ";"
        sql_query = sql_query.replace("and;", ";")
        f.write(sql_query + "\n")


def delete_all_event_programs(programs, f):
    programs = convert_to_sql_format(programs)
    f.write("--remove all events"+ "\n")
    f.write("delete from trackedentitydatavalueaudit where programstageinstanceid "
          "in ( select psi.programstageinstanceid  from programstageinstance psi "
          "inner join programstage ps on ps.programstageid=psi.programstageid "
          "inner join program p on p.programid=ps.programid "
          "where p.uid in " + programs + ");" + "\n")
    f.write("delete from programstageinstancecomments where programstageinstanceid "
          "in ( select psi.programstageinstanceid  from programstageinstance psi "
          "inner join programstage ps on ps.programstageid=psi.programstageid "
          "inner join program p on p.programid=ps.programid "
          "where p.uid in " + programs + ");" + "\n")
    f.write("delete from programstageinstance where programstageinstanceid "
          "in ( select psi.programstageinstanceid  from programstageinstance psi "
          "inner join programstage ps on ps.programstageid=psi.programstageid "
          "inner join program p on p.programid=ps.programid "
          "where p.uid in " + programs + ");" + "\n")


def delete_all_data_sets(datasets, f):
    datasets = convert_to_sql_format(datasets)
    f.write("--remove all datasets"+ "\n")
    f.write("delete from datavalueaudit where dataelementid in "
          "(select dataelementid from datasetelement "
          "where datasetid in (select datasetid from dataset where uid in " + datasets + "));" + "\n")
    f.write("delete from datavalue where dataelementid in "
          "(select dataelementid from datasetelement "
          "where datasetid in (select datasetid from dataset where uid in " + datasets + "));" + "\n")
    pass


def delete_all_tracker_programs(trackers, f):
    trackers = convert_to_sql_format(trackers)
    f.write("--remove all tracker"+ "\n")
    f.write("delete from trackedentitydatavalueaudit where programstageinstanceid "
          "in ( select psi.programstageinstanceid  from programstageinstance psi "
          "inner join programstage ps on ps.programstageid=psi.programstageid "
          "inner join program p on p.programid=ps.programid "
          "where p.uid in " + trackers + ");" + "\n")
    f.write(
        "delete from programstageinstancecomments where programstageinstanceid in ( select programstageinstanceid from programstageinstance where programstageid in ("
        "select programstageid from programstage where programid in (select programid from program where uid in " + trackers + ")));" + "\n")

    f.write("delete from trackedentityattributevalue where trackedentityinstanceid "
          "in ( select trackedentityinstanceid from programinstance where programid in(select programid from program where uid in " + trackers + "));" + "\n")
    f.write("delete from trackedentityattributevalueaudit where trackedentityinstanceid "
          "in ( select trackedentityinstanceid from programinstance where programid in(select programid from program where uid in " + trackers + "));" + "\n")

    f.write(
        "delete from programstageinstance where programstageid in ("
        "select programstageid from programstage where programid in (select programid from program where uid in " + trackers + "));" + "\n")

    f.write("delete from programstageinstance where programstageinstanceid "
          "in ( select psi.programstageinstanceid  from programstageinstance psi "
          "inner join programstage ps on ps.programstageid=psi.programstageid "
          "inner join program p on p.programid=ps.programid "
          "where p.uid in " + trackers + ");" + "\n")

    f.write(
        "create view tei_to_remove as select trackedentityinstanceid \"teiid\" from programinstance where programid in (select programid from program where uid in " + trackers + ");" + "\n")
    f.write(
        "delete from programinstance where programid in (select programid from program where uid in " + trackers + ");" + "\n")
    f.write("delete from programinstance where trackedentityinstanceid in ( select * from tei_to_remove);" + "\n")
    f.write("delete from trackedentityinstance where trackedentityinstanceid in ( select * from tei_to_remove);" + "\n")
    f.write("delete from trackedentityprogramowner where trackedentityinstanceid in ( select * from tei_to_remove);" + "\n")
    f.write("drop view tei_to_remove ;" + "\n")

    # f.write("delete from programstageinstancecomments where programstageinstanceid "
    #           "in ( select psi.programstageinstanceid  from programstageinstance psi "
    #           "inner join programstage ps on ps.programstageid=psi.programstageid "
    #           "inner join program p on p.programid=ps.programid "
    #           "where p.uid in " + trackers + ");")
    f.write("--remove tracker finish" + "\n")


def convert_to_sql_format(list_uid):
    if len(list_uid) == 0:
        return ""
    return "(" + ", ".join(["'{}'".format(uid) for uid in list_uid]) + ")"
