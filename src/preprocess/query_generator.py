import json


def remove_all_unnecessary_dependencies(f):
    f.write("""delete from programstageinstance_messageconversation;\n
    delete from programinstancecomments;\n
    delete from programinstanceaudit;\n
    delete from datavalueaudit;\n
    delete from trackedentitydatavalueaudit;\n
    delete from trackedentityattributevalueaudit;\n
    delete from programstageinstance_messageconversation;\n
    delete from dataapprovalaudit;\n
    delete from interpretation_comments;\n
    delete from interpretationcomment;\n
    delete from interpretationusergroupaccesses;\n
    delete from intepretation_likedby;\n
    delete from messageconversation_messages;\n
    delete from messageconversation_usermessages;\n
    delete from messageconversation;\n
    drop view if exists _view_nhwa_data_audit;
    drop view if exists _view_test2;\n""")


def generate_delete_event_rules(event_program, data_elements, org_units, org_unit_descendants, all_uid, f):
    f.write("--remove events\n")
    sql_all = convert_to_sql_format(all_uid)
    sql_event_program = convert_to_sql_format(event_program)
    sql_data_elements = convert_to_sql_format(data_elements)
    sql_org_units = convert_to_sql_format(org_units)
    sql_org_unit_descendants = convert_to_sql_format(org_unit_descendants)
    has_rule = False
    if sql_event_program != "":
        has_rule = True
        sql_query = "delete from programstageinstance where programstageid in " \
                    "(select programstageid from programstage where programid in " \
                    "(select programid from program where uid in {})) and".format(event_program)
    else:
        sql_query = "delete from programstageinstance where programstageid in " \
                    "(select programstageid from programstage where programid in " \
                    "(select programid from program where uid in {})) and".format(sql_all)
    if sql_data_elements != "":
        has_rule = True
        sql_data_elements = sql_data_elements.replace("(", "").replace(")", "")
        sql_query = "update programstageinstance set eventdatavalues = eventdatavalues - {} " \
                    "where eventdatavalues ? {} and ".format(sql_data_elements, sql_data_elements)
        if sql_event_program != "":
            sql_query = sql_query + " programstageid in (select programstageid from programstage " \
                                    "where programid in (select programid from program where uid in {})) and".format(event_program)
        else:
            sql_query = sql_query + " programstageid in (select programstageid from programstage " \
                                    "where programid in (select programid from program where uid in {})) and".format(sql_all)
    if sql_org_units != "":
        has_rule = True
        sql_query = sql_query + " organisationunitid in (select organisationunitid from organisationunit " \
                                "where uid in {}) and".format(sql_org_units)
    if sql_org_unit_descendants != "":
        has_rule = True
        sql_query = sql_query + " organisationunitid in (select organisationunitid from organisationunit " \
                                "where path like (select concat(path,'/%') from organisationunit " \
                                "where uid in {})) and".format(sql_org_unit_descendants)

    if not has_rule:
        delete_all_event_programs(all_uid, f)
    else:
        sql_query = sql_query + ";"
        sql_query = sql_query.replace("and;", ";")
        f.write(sql_query + "\n")


def generate_delete_tracker_rules(trackers, data_elements, org_units, org_unit_descendants, all_uid, f):
    f.write("--remove trackers \n")
    sql_all = convert_to_sql_format(all_uid)
    sql_trackers = convert_to_sql_format(trackers)
    sql_data_elements = convert_to_sql_format(data_elements)
    sql_org_units = convert_to_sql_format(org_units)
    sql_org_unit_descendants = convert_to_sql_format(org_unit_descendants)

    has_rule = False
    sql_query = "delete from programstageinstance where programstageid in " \
                "(select programstageid from programstage where programid in " \
                "(select programid from program where uid in {})) ".format(sql_all)
    if sql_data_elements != "":
        has_rule = True
        sql_data_elements = sql_data_elements.replace("(", "").replace(")", "")
        sql_query = "update programstageinstance set eventdatavalues = eventdatavalues - {}  where eventdatavalues ? " \
                    "{} and ".format(sql_data_elements, sql_data_elements)

        if sql_trackers != "":
            sql_query = sql_query + " programstageid in (select programstageid from programstage where programid in " \
                                    "(select programid from program where uid in {})) and".format(sql_trackers)
        else:
            sql_query = sql_query + " programstageid in (select programstageid from programstage where programid in " \
                                    "(select programid from program where uid in {})) and".format(sql_all)

    elif sql_trackers != "":
        has_rule = True
        sql_query = sql_query + " and dataelementid in (select dataelementid from datasetelement where datasetid in " \
                                "( select datasetid from dataset where uid in {})) ".format(sql_trackers)
    if sql_org_units != "":
        has_rule = True
        sql_query = sql_query + " and organisationunitid in (select organisationunitid from organisationunit where " \
                                "uid in {}) and".format(sql_org_units)
    if sql_org_unit_descendants != "":
        has_rule = True
        sql_query = sql_query + " and organisationunitid in (select organisationunitid from organisationunit where " \
                                "path like (select concat(path,'/%') from organisationunit where uid in {})) and".format(sql_org_unit_descendants)

    if not has_rule:
        delete_all_tracker_programs(all_uid, f)
    else:
        sql_query = sql_query + ";"
        sql_query = sql_query.replace("and;", ";")
        f.write(sql_query + "\n")


def delete_org_unit_data_in_view(f):
        f.write("""--remove organisationUnits -- data\n
        DELETE FROM programstageinstancecomments where programstageinstanceid in 
        (select programstageinstanceid from  programstageinstance where 
        organisationunitid in (select * from orgUnitsToDelete)); \n
        DELETE FROM programstageinstance where organisationunitid in (select * from orgUnitsToDelete);\n 
        delete from programinstance where organisationunitid in (select organisationunitid from orgUnitsToDelete); \n
        delete from datavalue where sourceid in (select organisationunitid from orgUnitsToDelete); \n
        delete from datavalueaudit where organisationunitid in (select organisationunitid  from orgUnitsToDelete); \n""")


def delete_org_units_in_view(f):
    f.write("""DELETE FROM trackedentitydatavalueaudit where programstageinstanceid in (select programstageinstanceid from  programstageinstance where organisationunitid in (select * from orgUnitsToDelete));\n
    DELETE FROM trackedentityattributevalue where trackedentityinstanceid in (select trackedentityinstanceid from  trackedentityinstance where organisationunitid in (select * from orgUnitsToDelete));\n
    DELETE FROM trackedentityattributevalueaudit where trackedentityinstanceid in (select trackedentityinstanceid from  trackedentityinstance where organisationunitid in (select * from orgUnitsToDelete));\n
    DELETE FROM datasetsource where sourceid in (select * from orgUnitsToDelete);\n
    DELETE FROM orgunitgroupmembers where organisationunitid in (select * from orgUnitsToDelete);\n
    DELETE FROM program_organisationunits where organisationunitid in (select * from orgUnitsToDelete);\n
    DELETE FROM  _orgunitstructure where organisationunitid in (select * from orgUnitsToDelete);\n
    DELETE FROM  _datasetorganisationunitcategory where organisationunitid in (select * from orgUnitsToDelete);\n
    DELETE FROM  _organisationunitgroupsetstructure where organisationunitid in (select * from orgUnitsToDelete);\n
    DELETE FROM  datavalueaudit where organisationunitid in (select * from orgUnitsToDelete);\n
    DELETE FROM  categoryoption_organisationunits where organisationunitid in (select * from orgUnitsToDelete);\n
    DELETE FROM  chart_organisationunits where organisationunitid in (select * from orgUnitsToDelete);\n
    DELETE FROM  dataapproval where organisationunitid in (select * from orgUnitsToDelete);\n
    DELETE FROM  dataapprovalaudit where organisationunitid in (select * from orgUnitsToDelete);\n
    DELETE FROM  eventchart_organisationunits where organisationunitid in (select * from orgUnitsToDelete);\n
    DELETE FROM  eventreport_organisationunits where organisationunitid in (select * from orgUnitsToDelete);\n
    DELETE FROM  interpretationuseraccesses where interpretationid in (select interpretationid from interpretation where organisationunitid in (select * from orgUnitsToDelete));\n
    DELETE FROM  interpretation_comments where interpretationid in (select interpretationid from interpretation where organisationunitid in (select * from orgUnitsToDelete));\n
    DELETE FROM  intepretation_likedby where interpretationid in (select interpretationid from interpretation where organisationunitid in (select * from orgUnitsToDelete));\n
    DELETE FROM  interpretationuseraccesses where interpretationid in (select interpretationid from interpretation where organisationunitid in (select * from orgUnitsToDelete));\n
    DELETE FROM  interpretation where organisationunitid in (select * from orgUnitsToDelete);\n
    DELETE FROM  lockexception where organisationunitid in (select * from orgUnitsToDelete);\n
    DELETE FROM  mapview_organisationunits where organisationunitid in (select * from orgUnitsToDelete);\n
    DELETE FROM  organisationunitattributevalues where organisationunitid in (select * from orgUnitsToDelete);\n
    DELETE FROM  program_organisationunits where organisationunitid in (select * from orgUnitsToDelete);\n
    DELETE FROM  programinstanceaudit where programinstanceid in (select programinstanceid from programinstance where organisationunitid in (select * from orgUnitsToDelete));\n
    DELETE FROM  programinstance where organisationunitid in (select * from orgUnitsToDelete);\n
    DELETE FROM  programmessage where organisationunitid in (select * from orgUnitsToDelete);\n
    DELETE FROM  reporttable_organisationunits where organisationunitid in (select * from orgUnitsToDelete);\n
    DELETE FROM trackedentityprogramowner WHERE organisationunitid in (select * from orgUnitsToDelete);\n
    DELETE FROM  trackedentityinstance where organisationunitid in (select * from orgUnitsToDelete);\n
    DELETE FROM  userdatavieworgunits where organisationunitid in (select * from orgUnitsToDelete);\n
    DELETE FROM  usermembership where organisationunitid in (select * from orgUnitsToDelete);\n
    DELETE FROM  userteisearchorgunits where organisationunitid in (select * from orgUnitsToDelete);\n
    DELETE FROM  validationresult where organisationunitid in (select * from orgUnitsToDelete);\n
    DELETE FROM completedatasetregistration where sourceid in (select * from orgUnitsToDelete);\n
    DELETE FROM configuration WHERE selfregistrationorgunit in (select * from orgUnitsToDelete);\n
    DELETE FROM minmaxdataelement WHERE sourceid in (select * from orgUnitsToDelete);\n
    DELETE FROM visualization_organisationunits WHERE organisationunitid in (select * from orgUnitsToDelete);\n
    DELETE FROM organisationunit WHERE organisationunitid in (select * from orgUnitsToDelete);\n
    DROP MATERIALIZED VIEW if exists orgUnitsToDelete;\n""")


def generate_delete_org_unit_level_rules(level, f):
    f.write("CREATE VIEW orgUnitsToDelete AS select organisationunitid from organisationunit where hierarchylevel > {} ; \n".format(level))
    delete_org_unit_data_in_view(f)
    delete_org_units_in_view(f)


def generate_delete_org_unit_tree_rules(orgunits, f):
    f.write("""--remove organisationUnits -- org unit\n
    DROP MATERIALIZED VIEW if exists orgUnitsToDelete;\n""")
    path_query = ""
    for org_unit in orgunits:
        path_query = " (path like '%{}%' and uid <> '{}') or ".format(org_unit, org_unit)
    path_query = path_query[:-3]
    f.write("CREATE MATERIALIZED VIEW orgUnitsToDelete AS select distinct organisationunitid from organisationunit where {} ; \n".format(path_query))
    delete_org_unit_data_in_view(f)
    delete_org_units_in_view(f)




def generate_delete_datasets_rules(datasets, data_elements, org_units, org_unit_descendants, all_uid, f):
    f.write("--remove datasets\n")
    sql_all = convert_to_sql_format(all_uid)
    sql_datasets = convert_to_sql_format(datasets)
    sql_data_elements = convert_to_sql_format(data_elements)
    sql_org_units = convert_to_sql_format(org_units)
    sql_org_unit_descendants = convert_to_sql_format(org_unit_descendants)
    sql_query = "delete from datavalue where dataelementid in (select dataelementid from datasetelement " \
                "where datasetid in ( select datasetid from dataset where uid in {})) and ".format(sql_all)
    has_rule = False
    if sql_data_elements != "":
        has_rule = True
        if sql_datasets != "":
            sql_query = sql_query + " dataelementid in (select dataelementid from dataelement where uid in {} " \
                                    "and dataelementid in (select dataelementid from datasetelement where datasetid " \
                                    "in ( select datasetid from dataset where uid in {})))" \
                                    " and".format(sql_data_elements, sql_datasets)
        else:
            sql_query = sql_query + " dataelementid in (select dataelementid from dataelement where uid in {} " \
                                    "and dataelementid in (select dataelementid from datasetelement " \
                                    "where datasetid in ( select datasetid from dataset where uid in {}))) " \
                                    "and".format(sql_data_elements, sql_all)
    elif sql_datasets != "":
        has_rule = True
        sql_query = sql_query + " dataelementid in (select dataelementid from datasetelement" \
                                " where datasetid in ( select datasetid from dataset where uid in {}))" \
                                " and".format(sql_datasets)
    if sql_org_units != "":
        has_rule = True
        sql_query = sql_query + " sourceid in (select organisationunitid from organisationunit where uid in {}) " \
                                "and".format(sql_org_units)
    if sql_org_unit_descendants != "":
        has_rule = True
        sql_query = sql_query + """ sourceid in (select organisationunitid from organisationunit where path " \
                                "like (select concat(path,'/%') from organisationunit where uid in {})) " \
                                "and""".format(sql_org_unit_descendants)

    if not has_rule:
        delete_all_data_sets(all_uid, f)
    else:
        sql_query = sql_query + ";"
        sql_query = sql_query.replace("and;", ";")
        f.write(sql_query + "\n")


def delete_all_event_programs(programs, f):
    programs = convert_to_sql_format(programs)
    f.write("--remove all events\n")
    f.write("delete from trackedentitydatavalueaudit where programstageinstanceid "
            "in ( select psi.programstageinstanceid  from programstageinstance psi "
            "inner join programstage ps on ps.programstageid=psi.programstageid "
            "inner join program p on p.programid=ps.programid where p.uid in {});\n".format(programs))
    f.write("delete from programstageinstancecomments where programstageinstanceid "
            "in ( select psi.programstageinstanceid  from programstageinstance psi "
            "inner join programstage ps on ps.programstageid=psi.programstageid "
            "inner join program p on p.programid=ps.programid where p.uid in {});\n".format(programs))
    f.write("delete from programstageinstance where programstageinstanceid "
            "in ( select psi.programstageinstanceid  from programstageinstance psi "
            "inner join programstage ps on ps.programstageid=psi.programstageid "
            "inner join program p on p.programid=ps.programid "
            "where p.uid in {});\n".format(programs))


def delete_all_data_sets(datasets, f):
    datasets = convert_to_sql_format(datasets)
    f.write("--remove all datasets\n")
    f.write("delete from datavalueaudit where dataelementid in "
            "(select dataelementid from datasetelement "
            "where datasetid in (select datasetid from dataset where uid in {}));\n".format(datasets))
    f.write("delete from datavalue where dataelementid in "
            "(select dataelementid from datasetelement "
            "where datasetid in (select datasetid from dataset where uid in {}));\n".format(datasets))
    pass


def delete_all_tracker_programs(trackers, f):
    trackers = convert_to_sql_format(trackers)
    f.write("--remove all tracker\n")
    f.write("delete from trackedentitydatavalueaudit where programstageinstanceid "
            "in ( select psi.programstageinstanceid  from programstageinstance psi "
            "inner join programstage ps on ps.programstageid=psi.programstageid "
            "inner join program p on p.programid=ps.programid "
            "where p.uid in {});\n".format(trackers))
    f.write(
        "delete from programstageinstancecomments where programstageinstanceid "
        "in ( select programstageinstanceid from programstageinstance where programstageid in ("
        "select programstageid from programstage where programid in "
        "(select programid from program where uid in {})));\n".format(trackers))

    f.write("delete from trackedentityattributevalue where trackedentityinstanceid "
            "in ( select trackedentityinstanceid from programinstance where programid in(select "
            "programid from program where uid in {}));\n".format(trackers))
    f.write("delete from trackedentityattributevalueaudit where trackedentityinstanceid "
            "in ( select trackedentityinstanceid from programinstance where programid "
            "in(select programid from program where uid in {}));\n".format(trackers))

    f.write(
        "delete from programstageinstance where programstageid in ("
        "select programstageid from programstage where programid in "
        "(select programid from program where uid in {}));\n".format(trackers))

    f.write("delete from programstageinstance where programstageinstanceid "
            "in ( select psi.programstageinstanceid  from programstageinstance psi "
            "inner join programstage ps on ps.programstageid=psi.programstageid "
            "inner join program p on p.programid=ps.programid "
            "where p.uid in {});\n".format(trackers))

    f.write(
        "create view tei_to_remove as select trackedentityinstanceid \"teiid\" "
        "from programinstance where programid in (select programid from program where uid in {});\n".format(trackers))
    f.write(
        "delete from programinstance where programid in (select programid from program where uid in {});\n".format(trackers))
    f.write("""delete from programinstance where trackedentityinstanceid in ( select * from tei_to_remove);\n
    delete from trackedentityinstance where trackedentityinstanceid in ( select * from tei_to_remove);\n
    delete from trackedentityprogramowner where trackedentityinstanceid in ( select * from tei_to_remove);\n
    drop view tei_to_remove ;\n
    --remove tracker finish\n""")


def convert_to_sql_format(list_uid):
    if len(list_uid) == 0:
        return ""
    return "(" + ", ".join(["'{}'".format(uid) for uid in list_uid]) + ")"
