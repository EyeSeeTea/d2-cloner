anonymize_coordinates = "'46.232889,6.13426'"
anonymize_email = "'user@example.com'"
anonymize_phone = "'+100001'"
anonymize_name = "'user'"
anonymize_value = "'value'"
anonymize_last_name = "'last_name'"

def remove_all_unnecessary_dependencies(f):
    write(f,"""delete from programstageinstance_messageconversation;\n""")
    write(f,"""delete from programinstancecomments;\n""")
    write(f,"""delete from programinstanceaudit;\n""")
    write(f,"""delete from datavalueaudit;\n""")
    write(f,"""delete from trackedentitydatavalueaudit;\n""")
    write(f,"""delete from trackedentityattributevalueaudit;\n""")
    write(f,"""delete from programstageinstance_messageconversation;\n""")
    write(f,"""delete from dataapprovalaudit;\n""")
    write(f,"""delete from interpretation_comments;\n""")
    write(f,"""delete from interpretationcomment;\n""")
    write(f,"""delete from messageconversation_messages;\n""")
    write(f,"""delete from messageconversation_usermessages;\n""")
    write(f,"""delete from messageconversation;\n""")


def generate_delete_event_rules(event_program, data_elements, org_units, org_unit_descendants, all_uid, f):
    write(f,"--remove events\n")
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
        write(f,sql_query + "\n")


def generate_delete_tracker_rules(trackers, data_elements, org_units, org_unit_descendants, all_uid, f):
    write(f,"--remove trackers \n")
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
        write(f,sql_query + "\n")


def generate_delete_datasets_rules(datasets, data_elements, org_units, org_unit_descendants, all_uid, f):
    write(f,"--remove datasets" + "\n")
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
        write(f,sql_query + "\n")


def delete_all_event_programs(programs, f):
    programs = convert_to_sql_format(programs)
    write(f,"--remove all events\n")
    write(f,"delete from trackedentitydatavalueaudit where programstageinstanceid "
            "in ( select psi.programstageinstanceid  from programstageinstance psi "
            "inner join programstage ps on ps.programstageid=psi.programstageid "
            "inner join program p on p.programid=ps.programid where p.uid in {});\n".format(programs))
    write(f,"delete from programstageinstancecomments where programstageinstanceid "
            "in ( select psi.programstageinstanceid  from programstageinstance psi "
            "inner join programstage ps on ps.programstageid=psi.programstageid "
            "inner join program p on p.programid=ps.programid where p.uid in {});\n".format(programs))
    write(f,"delete from programstageinstance where programstageinstanceid "
            "in ( select psi.programstageinstanceid  from programstageinstance psi "
            "inner join programstage ps on ps.programstageid=psi.programstageid "
            "inner join program p on p.programid=ps.programid "
            "where p.uid in {});\n".format(programs))


def delete_all_data_sets(datasets, f):
    datasets = convert_to_sql_format(datasets)
    write(f,"--remove all datasets\n")
    write(f,"delete from datavalueaudit where dataelementid in "
            "(select dataelementid from datasetelement "
            "where datasetid in (select datasetid from dataset where uid in {}));\n".format(datasets))
    write(f,"delete from datavalue where dataelementid in "
            "(select dataelementid from datasetelement "
            "where datasetid in (select datasetid from dataset where uid in {}));\n".format(datasets))
    pass


def delete_all_tracker_programs(trackers, f):
    trackers = convert_to_sql_format(trackers)
    write(f,"--remove all tracker\n")
    write(f,"delete from trackedentitydatavalueaudit where programstageinstanceid "
            "in ( select psi.programstageinstanceid  from programstageinstance psi "
            "inner join programstage ps on ps.programstageid=psi.programstageid "
            "inner join program p on p.programid=ps.programid "
            "where p.uid in {});\n".format(trackers))
    write(f,
        "delete from programstageinstancecomments where programstageinstanceid "
        "in ( select programstageinstanceid from programstageinstance where programstageid in ("
        "select programstageid from programstage where programid in "
        "(select programid from program where uid in {})));\n".format(trackers))

    write(f,"delete from trackedentityattributevalue where trackedentityinstanceid "
            "in ( select trackedentityinstanceid from programinstance where programid in(select "
            "programid from program where uid in {}));\n".format(trackers))
    write(f,"delete from trackedentityattributevalueaudit where trackedentityinstanceid "
            "in ( select trackedentityinstanceid from programinstance where programid "
            "in(select programid from program where uid in {}));\n".format(trackers))

    write(f,
        "delete from programstageinstance where programstageid in ("
        "select programstageid from programstage where programid in "
        "(select programid from program where uid in {}));\n".format(trackers))

    write(f,"delete from programstageinstance where programstageinstanceid "
            "in ( select psi.programstageinstanceid  from programstageinstance psi "
            "inner join programstage ps on ps.programstageid=psi.programstageid "
            "inner join program p on p.programid=ps.programid "
            "where p.uid in {});\n".format(trackers))

    write(f,
        "create view tei_to_remove as select trackedentityinstanceid \"teiid\" "
        "from programinstance where programid in (select programid from program where uid in {});\n".format(trackers))
    write(f,
        "delete from programinstance where programid in (select programid from program where uid in {});\n".format(trackers))
    write(f,"delete from programinstance where trackedentityinstanceid in ( select * from tei_to_remove);\n")
    write(f,"delete from trackedentityinstance where trackedentityinstanceid in ( select * from tei_to_remove);\n")
    write(f,"delete from trackedentityprogramowner where trackedentityinstanceid in ( select * from tei_to_remove);\n")
    write(f,"drop view tei_to_remove ;\n")
    write(f,"--remove tracker finish\n")


def anonymize_all_event_programs(programs, f):
    programs = convert_to_sql_format(programs)
    write(f,"--remove all events\n")
    write(f,"update programinstance set geometry=null, organisationunitid=(select organisationunitid "
            "from organisationunit ORDER BY RANDOM() limit 1) where programinstanceid "
            "in ( select psi.programinstanceid  from programstageinstance psi "
            "inner join programstage ps on ps.programstageid=psi.programstageid "
            "inner join program p on p.programid=ps.programid where p.uid in {});\n".format(programs))
    write(f,"update programstageinstance set geometry=null, organisationunitid=(select organisationunitid "
            "from organisationunit ORDER BY RANDOM() limit 1) where programstageinstanceid "
            "in ( select psi.programstageinstanceid  from programstageinstance psi "
            "inner join programstage ps on ps.programstageid=psi.programstageid "
            "inner join program p on p.programid=ps.programid "
            "where p.uid in {});\n".format(programs))


def anonymize_all_data_sets(datasets, f):
    datasets = convert_to_sql_format(datasets)
    write(f,"--anonymize all datasets\n")
    write(f,"update datavalueaudit as dv set organisationunitid=(select organisationunitid "
            "from organisationunit ORDER BY RANDOM()+"
            "dv.organisationunitid limit 1) where dataelementid in "
            "(select dataelementid from datasetelement "
            "where datasetid in (select datasetid from dataset where uid in {}));\n".format(datasets))
    write(f,"update datavalue as dv set sourceid=(select organisationunitid from organisationunit ORDER BY RANDOM()+"
            "dv.organisationunitid limit 1) where dataelementid in "
            "(select dataelementid from datasetelement "
            "where datasetid in (select datasetid from dataset where uid in {}));\n".format(datasets))
    pass


def anonymize_all_tracker_programs(programs, f):
    programs = convert_to_sql_format(programs)
    write(f, "--anonymize all tracker\n")
    write(f,"update programinstance as rand set geometry=null, organisationunitid=(select organisationunitid "
            "from organisationunit ORDER BY RANDOM()+rand.organisationunitid limit 1) where programinstanceid "
            "in ( select psi.programinstanceid  from programstageinstance psi "
            "inner join programstage ps on ps.programstageid=psi.programstageid "
            "inner join program p on p.programid=ps.programid where p.uid in {});\n".format(programs))
    write(f,"update programstageinstance as rand set geometry=null, coordinates='', organisationunitid="
            "(select organisationunitid from organisationunit ORDER BY RANDOM()+"
            "rand.organisationunitid limit 1) where programstageinstanceid "
            "in ( select psi.programstageinstanceid  from programstageinstance psi "
            "inner join programstage ps on ps.programstageid=psi.programstageid "
            "inner join program p on p.programid=ps.programid "
            "where p.uid in {});\n".format(programs))
    write(f,"update trackedentityinstance as rand set geometry=null, organisationunitid=(select "
            "organisationunitid from organisationunit ORDER BY RANDOM()+rand.organisationunitid "
            "limit 1) where trackedentityinstanceid "
            "in ( select ps.trackedentityinstanceid  from programinstance ps "
            "inner join program p on p.programid=ps.programid "
            "where p.uid in {});\n".format(programs))
    write(f,"update trackedentityprogramowner as rand set organisationunitid=(select organisationunitid from "
             "organisationunit ORDER BY RANDOM()+rand.organisationunitid limit 1) "
             "where trackedentityinstanceid in (select trackedentityinstanceid from "
            " program where uid in {});\n".format(programs))


def write(f, text):
    print(text)
    write(f,text)


def generate_anonymize_event_rules(event_program, data_elements, all_uid, f):
    write(f,"--anonymize events\n")
    sql_all = convert_to_sql_format(all_uid)
    sql_event_program = convert_to_sql_format(event_program)
    sql_data_elements = convert_to_sql_format(data_elements)
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

    if not has_rule:
        anonymize_all_event_programs(all_uid, f)
    else:
        sql_query = sql_query + ";"
        sql_query = sql_query.replace("and;", ";")
        write(f,sql_query + "\n")


def generate_anonymize_tracker_rules(trackers, tracker_attribute_values, data_elements, all_uid, f):
    write(f,"--anonymize trackers \n")
    sql_all = convert_to_sql_format(all_uid)
    sql_trackers = convert_to_sql_format(trackers)
    sql_tracker_entity_attributes = convert_to_sql_format(tracker_attribute_values)
    sql_data_elements = convert_to_sql_format(data_elements)
    list_of_tracker_uid = sql_trackers
    if sql_trackers == "":
        list_of_tracker_uid = sql_all

    anonymize_all_tracker_programs(list_of_tracker_uid, f)
    # update coordinates dataelements and trackedentityinstances
    #done
    write(f,"---coordinates\n")
    write(f,"UPDATE trackedentityattributevalue set value = {} where "
            "trackedentityinstanceid \n"
            "in ( select ps.trackedentityinstanceid  from programinstance ps \n"
            "inner join program p on p.programid=ps.programid \n"
            "where p.uid in {}) and trackedentityinstanceid in (select trackedentityattributeid from trackedentityattribute "
            "where valuetype like 'COORDINATE'); \n".format(anonymize_coordinates, list_of_tracker_uid))
    #done
    write(f,"UPDATE trackedentityattributevalue set value={} "
            "where trackedentityattributeid in (select trackedentityattributeid "
            "from trackedentityattribute where valuetype='COORDINATE') and "
            "trackedentityattributeid in (select trackedentityattributeid "
            "from trackedentityattribute inner join trackedentityinstance tei "
            "on tei.trackedentityinstanceid=trackedentityinstanceid inner join "
            "trackedentitytype tet on tet.trackedentitytypeid=tei.trackedentitytypeid "
            "inner join program p on tet.trackedentitytypeid=p.trackedentitytypeid "
            "where p.uid in {} );\n".format(anonymize_coordinates, list_of_tracker_uid))
    write(f,"---email\n")
    #done
    write(f,"UPDATE trackedentityattributevalue set value = {} where "
            "trackedentityinstanceid \n"
            "in ( select ps.trackedentityinstanceid  from programinstance ps \n"
            "inner join program p on p.programid=ps.programid \n"
            "where p.uid in {}) and trackedentityinstanceid in (select trackedentityattributeid from trackedentityattribute "
            "where valuetype like 'EMAIL'); \n".format(anonymize_email, list_of_tracker_uid))
    #done
    write(f,"UPDATE trackedentityattributevalue set value={} "
            "where trackedentityattributeid in (select trackedentityattributeid "
            "from trackedentityattribute where valuetype='EMAIL') and "
            "trackedentityattributeid in (select trackedentityattributeid "
            "from trackedentityattribute inner join trackedentityinstance tei "
            "on tei.trackedentityinstanceid=trackedentityinstanceid inner join "
            "trackedentitytype tet on tet.trackedentitytypeid=tei.trackedentitytypeid "
            "inner join program p on tet.trackedentitytypeid=p.trackedentitytypeid "
            "where p.uid in {} );\n".format(anonymize_email, list_of_tracker_uid))

    write(f,"---PHONE_NUMBER\n")
    #done
    write(f,"UPDATE trackedentityattributevalue set value = {} where "
            "trackedentityinstanceid \n"
            "in ( select ps.trackedentityinstanceid  from programinstance ps \n"
            "inner join program p on p.programid=ps.programid \n"
            "where p.uid in {}) and trackedentityinstanceid in (select trackedentityattributeid from trackedentityattribute "
            "where valuetype like 'PHONE_NUMBER'); \n".format(anonymize_phone, list_of_tracker_uid))
    #done
    write(f,"UPDATE trackedentityattributevalue set value={} "
            "where trackedentityattributeid in (select trackedentityattributeid "
            "from trackedentityattribute where valuetype='PHONE_NUMBER') and "
            "trackedentityattributeid in (select trackedentityattributeid "
            "from trackedentityattribute inner join trackedentityinstance tei "
            "on tei.trackedentityinstanceid=trackedentityinstanceid inner join "
            "trackedentitytype tet on tet.trackedentitytypeid=tei.trackedentitytypeid "
            "inner join program p on tet.trackedentitytypeid=p.trackedentitytypeid "
            "where p.uid in {} );\n".format(anonymize_phone, list_of_tracker_uid))
    # update email trackedentityinstances
    write(f,";DO\n"
            "$$DECLARE\n"
            " c CURSOR FOR SELECT * FROM trackedentityattributevalue where trackedentityinstanceid \n"
            "in ( select ps.trackedentityinstanceid  from programinstance ps \n"
            "inner join program p on p.programid=ps.programid \n"
            "where p.uid in {}) and trackedentityinstanceid in \n"
            "(select trackedentityinstanceid from trackedentityinstance where uid in {})"
            "and trackedentityinstanceid in (select trackedentityattributeid from trackedentityattribute"
            " where valuetype like 'EMAIL'); \n"
            " x trackedentityattributevalue;\n"
            " i integer := 1;\n"
            "BEGIN\n"
            " OPEN c;\n"
            " LOOP\n"
            " FETCH c INTO x;\n"
            " EXIT WHEN NOT FOUND;\n"
            " UPDATE trackedentityattributevalue SET value = '' || (i WHERE CURRENT OF c) || {};\n"
            " i := i + 1;\n"
            "END LOOP;\n"
            "END;$$;;\n".format(list_of_tracker_uid, sql_tracker_entity_attributes, anonymize_email))

    # update email dataelements
    write(f,";DO\n"
            "$$DECLARE\n"
            " c CURSOR FOR SELECT * FROM trackedentityattributevalue where trackedentityinstanceid \n"
            "in ( select ps.trackedentityinstanceid  from programinstance ps \n"
            "inner join program p on p.programid=ps.programid \n"
            "where p.uid in {}) and trackedentityinstanceid in \n"
            "(select trackedentityinstanceid from trackedentityinstance where uid in {})"
            "and trackedentityinstanceid in (select trackedentityattributeid from trackedentityattribute where valuetype like 'EMAIL'); \n"
            " x trackedentityattributevalue;\n"
            " i integer := 1;\n"
            "BEGIN\n"
            " OPEN c;\n"
            " LOOP\n"
            " FETCH c INTO x;\n"
            " EXIT WHEN NOT FOUND;\n"
            " UPDATE trackedentityattributevalue SET value = '' || (i WHERE CURRENT OF c) || {};\n"
            " i := i + 1;\n"
            "END LOOP;\n"
            "END;$$;;\n".format(list_of_tracker_uid, sql_tracker_entity_attributes, anonymize_email))
    # update phone trackedentityattributevalues

    #select teav.value, p.name, p.uid from trackedentityattributevalue teav inner join trackedentityinstance tei on tei.trackedentityinstanceid=teav.trackedentityinstanceid inner join trackedentitytype tet on tet.trackedentitytypeid=tei.trackedentitytypeid inner join program p on tet.trackedentitytypeid=p.trackedentitytypeid where teav.trackedentityattributeid in (select trackedentityattributeid from trackedentityattribute where valuetype='PHONE_NUMBER') and p.uid in {}
    write(f,";DO\n"
            "$$DECLARE\n"
            " c CURSOR FOR SELECT * FROM trackedentityattributevalue "
            "teav inner join trackedentityinstance tei on tei.trackedentityinstanceid=teav.trackedentityinstanceid "
            "inner join trackedentitytype tet on tet.trackedentitytypeid=tei.trackedentitytypeid inner join "
            "program p on tet.trackedentitytypeid=p.trackedentitytypeid where teav.trackedentityattributeid in "
            "(select trackedentityattributeid from trackedentityattribute where valuetype='PHONE_NUMBER'); "
            "and p.uid in {}; \n"
            " x trackedentityattributevalue;\n"
            " i integer := 1;\n"
            "BEGIN\n"
            " OPEN c;\n"
            " LOOP\n"
            " FETCH c INTO x;\n"
            " EXIT WHEN NOT FOUND;\n"
            " UPDATE trackedentityattributevalue SET value = (i WHERE CURRENT OF c) || {};\n"
            " i := i + 1;\n"
            "END LOOP;\n"
            "END;$$;;\n".format(list_of_tracker_uid, anonymize_phone))
    # update phone trackedentitytype values

    write(f,";DO\n"
            "$$DECLARE\n"
            " c CURSOR FOR SELECT * FROM trackedentityattributevalue where trackedentityinstanceid \n"
            "in ( select ps.trackedentityinstanceid  from programinstance ps \n"
            "inner join program p on p.programid=ps.programid \n"
            "where p.uid in {}) and trackedentityinstanceid in \n"
            "(select trackedentityinstanceid from trackedentityinstance where uid in {})"
            "and trackedentityinstanceid in (select trackedentityattributeid from trackedentityattribute where valuetype like 'PHONE_NUMBER'); \n"
            " x trackedentityattributevalue;\n"
            " i integer := 1;\n"
            "BEGIN\n"
            " OPEN c;\n"
            " LOOP\n"
            " FETCH c INTO x;\n"
            " EXIT WHEN NOT FOUND;\n"
            " UPDATE trackedentityattributevalue SET value = {} || i WHERE CURRENT OF c;\n"
            " i := i + 1;\n"
            "END LOOP;\n"
            "END;$$;;\n".format(list_of_tracker_uid, sql_tracker_entity_attributes, anonymize_phone))
    # update phone dataelements
    write(f,";DO\n"
            "$$DECLARE\n"
            " c CURSOR FOR SELECT * FROM trackedentityattributevalue where trackedentityinstanceid \n"
            "in ( select ps.trackedentityinstanceid  from programinstance ps \n"
            "inner join program p on p.programid=ps.programid \n"
            "where p.uid in {}) and trackedentityinstanceid in \n"
            "(select trackedentityinstanceid from trackedentityinstance where uid in {})"
            "and trackedentityinstanceid in (select trackedentityattributeid from trackedentityattribute where valuetype like 'PHONE_NUMBER'); \n"
            " x trackedentityattributevalue;\n"
            " i integer := 1;\n"
            "BEGIN\n"
            " OPEN c;\n"
            " LOOP\n"
            " FETCH c INTO x;\n"
            " EXIT WHEN NOT FOUND;\n"
            " UPDATE trackedentityattributevalue SET value = {} || i WHERE CURRENT OF c;\n"
            " i := i + 1;\n"
            "END LOOP;\n"
            "END;$$;;\n".format(list_of_tracker_uid, sql_tracker_entity_attributes, anonymize_phone))
    if sql_data_elements != "" or sql_tracker_entity_attributes != "":
        if sql_data_elements != "":
            sql_data_elements = sql_data_elements.replace("(", "").replace(")", "")
            sql_query = "update programstageinstance set eventdatavalues = eventdatavalues - {}  where eventdatavalues ? " \
                        "{} and ".format(sql_data_elements, sql_data_elements, list_of_tracker_uid)


        write(f,"---anonimize values \n")
        if sql_tracker_entity_attributes != "":
            write(f,";DO\n"
                    "$$DECLARE\n"
                    " c CURSOR FOR SELECT * FROM trackedentityattributevalue where trackedentityinstanceid \n"
                    "in ( select ps.trackedentityinstanceid  from programinstance ps \n"
                    "inner join program p on p.programid=ps.programid \n"
                    "where p.uid in {}) and trackedentityinstanceid in \n"
                    "(select trackedentityinstanceid from trackedentityinstance where uid in {}); \n"
                    " x trackedentityattributevalue;\n"
                    " i integer := 1;\n"
                    "BEGIN\n"
                    " OPEN c;\n"
                    " LOOP\n"
                    " FETCH c INTO x;\n"
                    " EXIT WHEN NOT FOUND;\n"
                    " UPDATE trackedentityattributevalue SET value = {} || i WHERE CURRENT OF c;\n"
                    " i := i + 1;\n"
                    "END LOOP;\n"
                    "END;$$;;\n".format(list_of_tracker_uid, sql_tracker_entity_attributes, anonymize_value))


def convert_to_sql_format(list_uid):
    if len(list_uid) == 0:
        return ""
    return "(" + ", ".join(["'{}'".format(uid) for uid in list_uid]) + ")"
