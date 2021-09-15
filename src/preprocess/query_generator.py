anonymize_email = "'@example.com'"


def remove_all_unnecessary_dependencies(f):
    write(f, """delete from programstageinstance_messageconversation;\n""")
    write(f, """delete from programinstancecomments;\n""")
    write(f, """delete from programinstanceaudit;\n""")
    write(f, """delete from datavalueaudit;\n""")
    write(f, """delete from trackedentitydatavalueaudit;\n""")
    write(f, """delete from trackedentityattributevalueaudit;\n""")
    write(f, """delete from programstageinstance_messageconversation;\n""")
    write(f, """delete from dataapprovalaudit;\n""")
    write(f, """delete from interpretation_comments;\n""")
    write(f, """delete from interpretationcomment;\n""")
    write(f, """delete from messageconversation_messages;\n""")
    write(f, """delete from messageconversation_usermessages;\n""")
    write(f, """delete from messageconversation;\n""")


def generate_delete_event_rules(event_program, data_elements, org_units, org_unit_descendants, all_uid, f):
    write(f, "--remove events\n")
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
                                    "where programid in (select programid from program where uid in {})) and".format(
                event_program)
        else:
            sql_query = sql_query + " programstageid in (select programstageid from programstage " \
                                    "where programid in (select programid from program where uid in {})) and".format(
                sql_all)
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
        write(f, sql_query + "\n")


def generate_delete_tracker_rules(trackers, data_elements, org_units, org_unit_descendants, all_uid, f):
    write(f, "--remove trackers \n")
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
                                "path like (select concat(path,'/%') from organisationunit where uid in {})) and".format(
            sql_org_unit_descendants)

    if not has_rule:
        delete_all_tracker_programs(all_uid, f)
    else:
        sql_query = sql_query + ";"
        sql_query = sql_query.replace("and;", ";")
        write(f, sql_query + "\n")


def generate_delete_datasets_rules(datasets, data_elements, org_units, org_unit_descendants, all_uid, f):
    write(f, "--remove datasets" + "\n")
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
        write(f, sql_query + "\n")


def delete_all_event_programs(programs, f):
    programs = convert_to_sql_format(programs)
    write(f, "--remove all events\n")
    write(f, "delete from trackedentitydatavalueaudit where programstageinstanceid "
             "in ( select psi.programstageinstanceid  from programstageinstance psi "
             "inner join programstage ps on ps.programstageid=psi.programstageid "
             "inner join program p on p.programid=ps.programid where p.uid in {});\n".format(programs))
    write(f, "delete from programstageinstancecomments where programstageinstanceid "
             "in ( select psi.programstageinstanceid  from programstageinstance psi "
             "inner join programstage ps on ps.programstageid=psi.programstageid "
             "inner join program p on p.programid=ps.programid where p.uid in {});\n".format(programs))
    write(f, "delete from programstageinstance where programstageinstanceid "
             "in ( select psi.programstageinstanceid  from programstageinstance psi "
             "inner join programstage ps on ps.programstageid=psi.programstageid "
             "inner join program p on p.programid=ps.programid "
             "where p.uid in {});\n".format(programs))


def delete_all_data_sets(datasets, f):
    datasets = convert_to_sql_format(datasets)
    write(f, "--remove all datasets\n")
    write(f, "delete from datavalueaudit where dataelementid in "
             "(select dataelementid from datasetelement "
             "where datasetid in (select datasetid from dataset where uid in {}));\n".format(datasets))
    write(f, "delete from datavalue where dataelementid in "
             "(select dataelementid from datasetelement "
             "where datasetid in (select datasetid from dataset where uid in {}));\n".format(datasets))
    pass


def delete_all_tracker_programs(trackers, f):
    trackers = convert_to_sql_format(trackers)
    write(f, "--remove all tracker\n")
    write(f, "delete from trackedentitydatavalueaudit where programstageinstanceid "
             "in ( select psi.programstageinstanceid  from programstageinstance psi "
             "inner join programstage ps on ps.programstageid=psi.programstageid "
             "inner join program p on p.programid=ps.programid "
             "where p.uid in {});\n".format(trackers))
    write(f,
          "delete from programstageinstancecomments where programstageinstanceid "
          "in ( select programstageinstanceid from programstageinstance where programstageid in ("
          "select programstageid from programstage where programid in "
          "(select programid from program where uid in {})));\n".format(trackers))

    write(f, "delete from trackedentityattributevalue where trackedentityinstanceid "
             "in ( select trackedentityinstanceid from programinstance where programid in(select "
             "programid from program where uid in {}));\n".format(trackers))
    write(f, "delete from trackedentityattributevalueaudit where trackedentityinstanceid "
             "in ( select trackedentityinstanceid from programinstance where programid "
             "in(select programid from program where uid in {}));\n".format(trackers))

    write(f,
          "delete from programstageinstance where programstageid in ("
          "select programstageid from programstage where programid in "
          "(select programid from program where uid in {}));\n".format(trackers))

    write(f, "delete from programstageinstance where programstageinstanceid "
             "in ( select psi.programstageinstanceid  from programstageinstance psi "
             "inner join programstage ps on ps.programstageid=psi.programstageid "
             "inner join program p on p.programid=ps.programid "
             "where p.uid in {});\n".format(trackers))

    write(f,
          "create view tei_to_remove as select trackedentityinstanceid \"teiid\" "
          "from programinstance where programid in (select programid from program where uid in {});\n".format(trackers))
    write(f,
          "delete from programinstance where programid in (select programid from program where uid in {});\n".format(
              trackers))
    write(f, "delete from programinstance where trackedentityinstanceid in ( select * from tei_to_remove);\n")
    write(f, "delete from trackedentityinstance where trackedentityinstanceid in ( select * from tei_to_remove);\n")
    write(f, "delete from trackedentityprogramowner where trackedentityinstanceid in ( select * from tei_to_remove);\n")
    write(f, "drop view tei_to_remove ;\n")
    write(f, "--remove tracker finish\n")


def anonymize_all_data_sets(datasets, f):
    datasets = convert_to_sql_format(datasets)
    write(f, "--anonymize all datasets\n")
    sql_query = "UPDATE datavalue " \
                "SET value = compare.update " \
                "FROM ( SELECT dataelementid AS lookup FROM datasetelement WHERE datasetid IN ( " \
                "SELECT datasetid FROM dataset WHERE uid IN {} ) as valid_dataelements " \
                "CROSS JOIN LATERAL ( " \
                "SELECT t1.dataelementid, t1.sourceid, t1.periodid, t1.categoryoptioncomboid, t1.attributeoptioncomboid, t1.actual, t2.update " \
                "FROM (SELECT row_number() OVER () AS index, dataelementid, sourceid, periodid, categoryoptioncomboid, attributeoptioncomboid, value AS actual " \
                "FROM datavalue t1 " \
                "WHERE t1.dataelementid = lookup " \
                ") t1 JOIN (SELECT row_number() OVER (ORDER BY random()) AS index, value AS update FROM datavalue t2 WHERE t2.dataelementid = lookup ) t2 USING (index) " \
                ") AS lookup " \
                ") compare " \
                "WHERE datavalue.dataelementid = compare.dataelementid AND datavalue.sourceid = compare.sourceid AND " \
                "datavalue.periodid = compare.periodid AND " \
                "datavalue.categoryoptioncomboid = compare.categoryoptioncomboid AND " \
                "datavalue.attributeoptioncomboid = compare.attributeoptioncomboid;".format(datasets)
    write(f, sql_query)


def anonymize_all_tracker_programs(programs, f):
    programs = convert_to_sql_format(programs)
    write(f, "--anonymize all tracker\n")
    write(f, "---remove event coor orgunits and phonenumbers\n")
    write(f,
          "UPDATE programstageinstance SET "
          "eventdatavalues = eventdatavalues - array(SELECT uid FROM   dataelement WHERE "
          "( valuetype = 'PHONE_NUMBER' OR valuetype = 'COORDINATES' ) "
          "AND dataelementid IN"
          "(SELECT psde.dataelementid FROM   program p INNER JOIN programstage ps "
          "ON p.programid = ps.programid INNER JOIN programstagedataelement psde "
          "ON psde.programstageid = ps.programstageid WHERE "
          " p.uid IN {})) "
          "WHERE  eventdatavalues ?| array(SELECT uid FROM   dataelement "
          "WHERE  ( valuetype = 'PHONE_NUMBER' OR valuetype = 'COORDINATES' OR valuetype = 'ORGANISATION_UNIT' ) "
          "AND dataelementid IN (SELECT psde.dataelementid"
          " FROM program p INNER JOIN programstage ps ON p.programid = ps.programid INNER JOIN "
          "programstagedataelement psde ON psde.programstageid = ps.programstageid WHERE  p.uid IN {})); \n"
          .format(programs, programs))
    write(f,
          "UPDATE programstageinstance SET "
          "eventdatavalues = eventdatavalues - array(SELECT uid FROM   dataelement WHERE "
          "( valuetype = 'PHONE_NUMBER' OR valuetype = 'COORDINATES') "
          "AND dataelementid IN"
          "(SELECT psde.dataelementid FROM   program p INNER JOIN programstage ps "
          "ON p.programid = ps.programid INNER JOIN programstagedataelement psde "
          "ON psde.programstageid = ps.programstageid WHERE "
          " p.uid IN {})) "
          "WHERE  eventdatavalues ?| array(SELECT uid FROM   dataelement "
          "WHERE  ( valuetype = 'PHONE_NUMBER' OR valuetype = 'COORDINATES' OR valuetype = 'ORGANISATION_UNIT' ) "
          "AND dataelementid IN (SELECT psde.dataelementid"
          " FROM program p INNER JOIN programstage ps ON p.programid = ps.programid INNER JOIN "
          "programstagedataelement psde ON psde.programstageid = ps.programstageid WHERE  p.uid IN {})); \n"
          .format(programs, programs))

    write(f, "delete from trackedentityattributevalue where "
             "trackedentityinstanceid \n"
             "in ( select ps.trackedentityinstanceid  from programinstance ps \n"
             "inner join program p on p.programid=ps.programid \n"
             "where p.uid in {}) and trackedentityinstanceid in (select trackedentityattributeid from trackedentityattribute "
             "where valuetype like 'COORDINATE'); \n".format(programs))

    write(f, "delete from trackedentityattributevalue where "
             "trackedentityinstanceid \n"
             "in ( select ps.trackedentityinstanceid  from programinstance ps \n"
             "inner join program p on p.programid=ps.programid \n"
             "where p.uid in {}) and trackedentityinstanceid in (select trackedentityattributeid from trackedentityattribute "
             "where valuetype like 'PHONE_NUMBER'); \n".format(programs))

    write(f, "---email\n")
    write(f,
          "UPDATE trackedentityattributevalue set value=('randomuser' || round(random()*trackedentityinstanceid+trackedentityattributeid)::text || {}) "
          "where trackedentityattributeid in (select trackedentityattributeid "
          "from trackedentityattribute where valuetype='EMAIL') and "
          "trackedentityinstanceid in (select trackedentityinstanceid "
          "from trackedentityattribute inner join trackedentityinstance tei "
          "on tei.trackedentityinstanceid=trackedentityinstanceid inner join "
          "trackedentitytype tet on tet.trackedentitytypeid=tei.trackedentitytypeid "
          "inner join program p on tet.trackedentitytypeid=p.trackedentitytypeid "
          "where p.uid in {} );\n".format(anonymize_email, programs))
    write(f,
          "UPDATE trackedentityattributevalue set value=('Redacted ' || round(random()*trackedentityinstanceid+trackedentityattributeid)::text) "
          "where trackedentityattributeid in (select trackedentityattributeid "
          "from trackedentityattribute where valuetype='TEXT' or valuetype='LONG_TEXT') and "
          "trackedentityinstanceid in (select trackedentityinstanceid "
          "from trackedentityattribute inner join trackedentityinstance tei "
          "on tei.trackedentityinstanceid=trackedentityinstanceid inner join "
          "trackedentitytype tet on tet.trackedentitytypeid=tei.trackedentitytypeid "
          "inner join program p on tet.trackedentitytypeid=p.trackedentitytypeid "
          "where p.uid in {} );\n".format(programs))


def write(f, text):
    print(text)
    f.write(text)


def generate_anonymize_datasets_rules(dataset_uids, org_units, data_elements,
                                anonimize_org_units, anonimize_phone, anonimize_mail,
                                anonimize_coordinate,  all_uid, f):
    write(f, "--anonymize datasets\n")
    sql_all = convert_to_sql_format(all_uid)
    sql_datasets_uids = convert_to_sql_format(dataset_uids)
    sql_data_elements = convert_to_sql_format(data_elements)
    sql_org_units = convert_to_sql_format(org_units)
    where_dataelements = ""

    if sql_datasets_uids != "":
        datasets = sql_datasets_uids
    else:
        datasets = sql_all

    where_datasets = "SELECT dataelementid AS lookup FROM datasetelement WHERE datasetid IN ( " \
                     "SELECT datasetid FROM dataset WHERE uid IN {} ".format(datasets)

    if sql_data_elements != "":
        where_dataelements = " and uid in {} ".format(
            sql_data_elements)

        write(f,
              "Update datavalue set value=concat(\'Redacted:\',round(random()*dataelementid+categoryoptioncomboid+sourceid))"
              " where dataelementid in (select dataelementid from dataelement where valuetype='TEXT' or valuetype='LONG_TEXT')" \
              " and dataelementid in (select dataelementid from dataelement where uid in {}); ".format(sql_data_elements))

    if sql_org_units != "":
        where_orgunits = " and sourceid in (select organisationunitid from organisationuint where uid in {})".format(sql_org_units)
    else:
        where_orgunits = ""

    where_clausules = where_datasets + where_dataelements + where_orgunits
    if anonimize_org_units:
        has_rule = True
        sql_query = "UPDATE datavalue " \
                    "SET value = compare.update,  comment='' " \
                    "FROM ( {} ) as valid_dataelements " \
                    "CROSS JOIN LATERAL ( " \
                    "SELECT t1.dataelementid, t1.sourceid, t1.periodid, t1.categoryoptioncomboid, t1.attributeoptioncomboid, t1.actual, t2.update " \
                    "FROM (SELECT row_number() OVER () AS " \
                    "index, dataelementid, sourceid, periodid, categoryoptioncomboid, attributeoptioncomboid, value AS actual " \
                    "FROM datavalue t1 " \
                    "WHERE t1.dataelementid = lookup " \
                    ") t1 JOIN (SELECT row_number() OVER (ORDER BY random()) AS index, value AS update FROM datavalue t2 WHERE t2.dataelementid = lookup ) t2 USING (index) " \
                    ") AS lookup " \
                    ") compare " \
                    "WHERE datavalue.dataelementid = compare.dataelementid AND datavalue.sourceid = compare.sourceid AND " \
                    "datavalue.periodid = compare.periodid AND " \
                    "datavalue.categoryoptioncomboid = compare.categoryoptioncomboid AND " \
                    "datavalue.attributeoptioncomboid = compare.attributeoptioncomboid;"

        where = "SELECT * FROM ( {} )".format(where_clausules)
        sql_query = sql_query.format(where)
        write(f, sql_query + "\n")
    else:
        sql_query = "update datavalue set comment = '' where " \
                    " dataelementid in (select dataelementid from datasetelement where datasetid " \
                    "in (select datasetid from dataset where uid in {}));".format(datasets)
        write(f, sql_query + "\n")

    if anonimize_phone:
        write(f,
              "Update datavalue set value=concat(\'+\',round(random()*dataelementid+categoryoptioncomboid+sourceid))"
              " where dataelementid in (select dataelementid from dataelement where valuetype='PHONE_NUMBER')" \
              " and dataelementid in ({})); ".format(where_clausules))
    if anonimize_mail:
        write(f,
              "Update datavalue set value=concat(\'user\',round(random()*dataelementid+categoryoptioncomboid+sourceid)) || '@example.com'"
              " where dataelementid in (select dataelementid from dataelement where valuetype='PHONE_NUMBER')" \
              " and dataelementid in ({})); ".format(where_clausules))
    if anonimize_coordinate:
        write(f,
              "DELETE FROM datavalue where dataelementid in (select dataelementid from dataelement where valuetype='COORDINATE')" \
              " and dataelementid in ({})); ".format(where_clausules))
    if anonimize_org_units:
        write(f,
              "DELETE FROM datavalue where dataelementid in (select dataelementid from dataelement where valuetype='ORGANISATION_UNIT')" \
              " and dataelementid in ({})); ".format(where_clausules))


def generate_anonymize_event_rules(event_program, organisationunits, data_elements,
                                   anonimize_org_units, anonimize_phone, anonimize_mail,
                                   anonimize_coordinate, all_uid, f):
    write(f, "--anonymize events\n")
    if event_program != "":
        sql_event_program = convert_to_sql_format(event_program)
    else:
        sql_event_program = convert_to_sql_format(all_uid)
    sql_data_elements = convert_to_sql_format(data_elements)

    if anonimize_mail:
        write(f,
              "UPDATE programstageinstance SET "
              "eventdatavalues = eventdatavalues - array(SELECT uid FROM   dataelement WHERE "
              " valuetype = 'EMAIL' "
              "AND dataelementid IN"
              "(SELECT psde.dataelementid FROM   program p INNER JOIN programstage ps "
              "ON p.programid = ps.programid INNER JOIN programstagedataelement psde "
              "ON psde.programstageid = ps.programstageid WHERE "
              " p.uid IN {})) "
              "WHERE eventdatavalues ?| array(SELECT uid FROM   dataelement "
              "WHERE   valuetype = 'PHONE_NUMBER' "
              " AND dataelementid IN (SELECT psde.dataelementid"
              " FROM program p INNER JOIN programstage ps ON p.programid = ps.programid INNER JOIN "
              "programstagedataelement psde ON psde.programstageid = ps.programstageid WHERE  p.uid IN {})); \n"
              .format(sql_event_program, sql_event_program))
    if anonimize_phone:
        write(f,
              "UPDATE programstageinstance SET "
              "eventdatavalues = eventdatavalues - array(SELECT uid FROM   dataelement WHERE "
              " valuetype = 'PHONE_NUMBER' "
              "AND dataelementid IN"
              "(SELECT psde.dataelementid FROM   program p INNER JOIN programstage ps "
              "ON p.programid = ps.programid INNER JOIN programstagedataelement psde "
              "ON psde.programstageid = ps.programstageid WHERE "
              " p.uid IN {})) "
              "WHERE eventdatavalues ?| array(SELECT uid FROM   dataelement "
              "WHERE   valuetype = 'PHONE_NUMBER' "
              " AND dataelementid IN (SELECT psde.dataelementid"
              " FROM program p INNER JOIN programstage ps ON p.programid = ps.programid INNER JOIN "
              "programstagedataelement psde ON psde.programstageid = ps.programstageid WHERE  p.uid IN {})); \n"
              .format(sql_event_program, sql_event_program))
    if anonimize_coordinate:
        write(f, "update programinstance as rand set geometry=null where programinstanceid "
                 "in ( select psi.programinstanceid  from programstageinstance psi "
                 "inner join programstage ps on ps.programstageid=psi.programstageid "
                 "inner join program p on p.programid=ps.programid where p.uid in {});\n".format(sql_event_program))
        write(f, "update programstageinstance as rand set geometry=null where programstageinstanceid "
                 "in ( select psi.programstageinstanceid  from programstageinstance psi "
                 "inner join programstage ps on ps.programstageid=psi.programstageid "
                 "inner join program p on p.programid=ps.programid "
                 "where p.uid in {});\n".format(sql_event_program))
    if anonimize_org_units:
        write(f, "update programinstance as rand set organisationunitid=(select organisationunitid "
                 "from organisationunit where length(path)=(select length(path) from organisationunit where rand.organisationunitid= organisationunitid) ORDER BY RANDOM() limit 1) where programinstanceid "
                 "in ( select psi.programinstanceid  from programstageinstance psi "
                 "inner join programstage ps on ps.programstageid=psi.programstageid "
                 "inner join program p on p.programid=ps.programid where p.uid in {});\n".format(sql_event_program))
        write(f, "update programstageinstance as rand set organisationunitid="
                 "(select organisationunitid from organisationunit where length(path)=(select length(path) from organisationunit where rand.organisationunitid= organisationunitid) ORDER BY RANDOM()"
                 "limit 1) where programstageinstanceid "
                 "in ( select psi.programstageinstanceid  from programstageinstance psi "
                 "inner join programstage ps on ps.programstageid=psi.programstageid "
                 "inner join program p on p.programid=ps.programid "
                 "where p.uid in {});\n".format(sql_event_program))

    if sql_data_elements != "":
        if sql_data_elements != "":
            for uid in data_elements:
                write(f, "UPDATE programstageinstance SET    eventdatavalues = jsonb_set(eventdatavalues, "
                         "'{" + uid + ",value}', concat(\'\',concat(concat(\'" + "\"" + "\',(select concat(\'Redacted:\',"
                         "round(random()*dataelementid+programstageinstanceid))"
                         ' FROM   dataelement WHERE  uid = \'sPadHOO4SQY\'),\'' + '\"' + '\'),\'\'))::jsonb)'
                         ' WHERE  programstageinstanceid in (select programstageinstanceid from programstageinstance'
                         ' where  eventdatavalues ? (SELECT de.uid FROM   dataelement as de'
                         ' where  ( de.valuetype = \'TEXT\' OR de.valuetype = \'LONG_TEXT\' )'
                         ' AND de.dataelementid IN (SELECT psde.dataelementid'
                         ' FROM program p INNER JOIN programstage ps ON p.programid = ps.programid INNER JOIN'
                         ' programstagedataelement psde ON psde.programstageid = ps.programstageid WHERE  p.uid IN (\'' + "', '".join(
                    all_uid) + '\')) and de.uid = \'' + uid + '\'));')


def generate_anonymize_tracker_rules(trackers, tracker_attribute_values, organisationunits, data_elements,
                                     anonimize_org_units, anonimize_phone, anonimize_mail,
                                     anonimize_coordinate, all_uid, f):
    write(f, "--anonymize trackers \n")
    if trackers != "":
        sql_trackers = convert_to_sql_format(trackers)
    else:
        sql_trackers = convert_to_sql_format(all_uid)
    sql_organisationunits = convert_to_sql_format(organisationunits)
    sql_tracker_entity_attributes = convert_to_sql_format(tracker_attribute_values)
    sql_data_elements = convert_to_sql_format(data_elements)

    where = " and trackedentityinstanceid \n " \
            "in ( select ps.trackedentityinstanceid  " \
            "from programinstance ps \n" \
            "inner join program p on p.programid=ps.programid \n" \
            "where p.uid in {}) \n".format(sql_trackers)

    if sql_data_elements != "":
        for uid in data_elements:
            write(f, "UPDATE programstageinstance SET    eventdatavalues = jsonb_set(eventdatavalues, "
                     "'{" + uid + ",value}', concat(\'\',concat(concat(\'" + "\"" + "\',(select concat(\'random\',"
                     "round(random()*dataelementid+programstageinstanceid))"
                     ' FROM   dataelement WHERE  uid = \'sPadHOO4SQY\'),\'' + '\"' + '\'),\'\'))::jsonb)'
                     ' WHERE  programstageinstanceid in (select programstageinstanceid from programstageinstance'
                     ' where  eventdatavalues ? (SELECT de.uid FROM   dataelement as de'
                     ' where  ( de.valuetype = \'TEXT\' OR de.valuetype = \'LONG_TEXT\' )'
                     ' AND de.dataelementid IN (SELECT psde.dataelementid'
                     ' FROM program p INNER JOIN programstage ps ON p.programid = ps.programid INNER JOIN'
                     ' programstagedataelement psde ON psde.programstageid = ps.programstageid WHERE  p.uid IN '
                  + sql_trackers + ') and de.uid = \'' + uid + '\'));')

    if sql_tracker_entity_attributes != "":
        write(f,
              "UPDATE trackedentityattributevalue set "
              "value=('Redacted ' || round(random()*trackedentityinstanceid+trackedentityattributeid)::text) "
              "where trackedentityattributeid in (select trackedentityattributeid "
              "from trackedentityattribute where valuetype='TEXT' or valuetype='LONG_TEXT')"
              " and trackedentityattributeid in (select trackedentityattributeid "
              "from trackedentityattribute where uid in {} );\n"
              .format(sql_tracker_entity_attributes))

    if sql_organisationunits != "":
        where = where + " and organisationunitid \n" \
                        "in (select organisationunitid  from organisationunit " \
                        "where uid in {})  \n".format(sql_organisationunits)
    if anonimize_coordinate:
        write(f, "update programinstance as rand set geometry=null where programinstanceid "
                 "in ( select psi.programinstanceid  from programstageinstance psi "
                 "inner join programstage ps on ps.programstageid=psi.programstageid "
                 "inner join program p on p.programid=ps.programid where p.uid in {});\n".format(sql_trackers))
        write(f,
              "update programstageinstance as rand set geometry=null where programstageinstanceid "
              "in ( select psi.programstageinstanceid  from programstageinstance psi "
              "inner join programstage ps on ps.programstageid=psi.programstageid "
              "inner join program p on p.programid=ps.programid "
              "where p.uid in {});\n".format(sql_trackers))
        write(f,
              "update trackedentityinstance as rand set coordinates=null where trackedentityinstanceid "
              "in ( select ps.trackedentityinstanceid  from programinstance ps "
              "inner join program p on p.programid=ps.programid "
              "where p.uid in {});\n".format(sql_trackers))
        write(f,
              "UPDATE programstageinstance SET "
              "eventdatavalues = eventdatavalues - array(SELECT uid FROM   dataelement WHERE "
              " valuetype = 'COORDINATES' "
              "AND dataelementid IN"
              "(SELECT psde.dataelementid FROM   program p INNER JOIN programstage ps "
              "ON p.programid = ps.programid INNER JOIN programstagedataelement psde "
              "ON psde.programstageid = ps.programstageid WHERE "
              " p.uid IN {})) "
              "WHERE eventdatavalues ?| array(SELECT uid FROM   dataelement "
              "WHERE   valuetype = 'COORDINATES' "
              " AND dataelementid IN (SELECT psde.dataelementid"
              " FROM program p INNER JOIN programstage ps ON p.programid = ps.programid INNER JOIN "
              "programstagedataelement psde ON psde.programstageid = ps.programstageid WHERE  p.uid IN {})); \n"
              .format(sql_trackers, sql_trackers))

        # done delete coordinate
        write(f, "delete from trackedentityattributevalue where "
                 "trackedentityinstanceid in (select trackedentityattributeid from trackedentityattribute "
                 "where valuetype like 'COORDINATE') {}; \n".format(where))
    if anonimize_org_units:
        write(f, "update programinstance as rand set organisationunitid=(select organisationunitid "
                 "from organisationunit where length(path)=(select length(path) from organisationunit where "
                 "rand.organisationunitid= organisationunitid) ORDER BY RANDOM() limit 1) where programinstanceid "
                 "in ( select psi.programinstanceid  from programstageinstance psi "
                 "inner join programstage ps on ps.programstageid=psi.programstageid "
                 "inner join program p on p.programid=ps.programid where p.uid in {});\n".format(sql_trackers))
        write(f, "update programstageinstance as rand set organisationunitid="
                 "(select organisationunitid from organisationunit where length(path)=(select length(path) from "
                 "organisationunit where rand.organisationunitid= organisationunitid) ORDER BY RANDOM()"
                 "limit 1) where programstageinstanceid "
                 "in ( select psi.programstageinstanceid  from programstageinstance psi "
                 "inner join programstage ps on ps.programstageid=psi.programstageid "
                 "inner join program p on p.programid=ps.programid "
                 "where p.uid in {});\n".format(sql_trackers))
        write(f, "update trackedentityinstance as rand set organisationunitid=(select "
                 "organisationunitid from organisationunit where length(path)=(select length(path) from "
                 "organisationunit where rand.organisationunitid= organisationunitid) ORDER BY RANDOM() "
                 "limit 1) where trackedentityinstanceid "
                 "in ( select ps.trackedentityinstanceid  from programinstance ps "
                 "inner join program p on p.programid=ps.programid "
                 "where p.uid in {});\n".format(sql_trackers))
        write(f, "update trackedentityprogramowner as rand set organisationunitid=(select organisationunitid from "
                 "organisationunit where length(path)=(select length(path) from organisationunit where "
                 "rand.organisationunitid= organisationunitid) ORDER BY RANDOM() limit 1) "
                 "where trackedentityinstanceid in (select trackedentityinstanceid from "
                 " program where uid in {});\n".format(sql_trackers))
        write(f, "delete from trackedentityattributevalue where "
                 "trackedentityinstanceid in (select trackedentityattributeid from trackedentityattribute "
                 "where valuetype like 'ORGANISATION_UNIT') {}; \n".format(where))
        write(f,
              "UPDATE programstageinstance SET "
              "eventdatavalues = eventdatavalues - array(SELECT uid FROM   dataelement WHERE "
              " valuetype = 'ORGANISATION_UNIT' "
              "AND dataelementid IN"
              "(SELECT psde.dataelementid FROM   program p INNER JOIN programstage ps "
              "ON p.programid = ps.programid INNER JOIN programstagedataelement psde "
              "ON psde.programstageid = ps.programstageid WHERE "
              " p.uid IN {})) "
              "WHERE eventdatavalues ?| array(SELECT uid FROM   dataelement "
              "WHERE   valuetype = 'ORGANISATION_UNIT' "
              " AND dataelementid IN (SELECT psde.dataelementid"
              " FROM program p INNER JOIN programstage ps ON p.programid = ps.programid INNER JOIN "
              "programstagedataelement psde ON psde.programstageid = ps.programstageid WHERE  p.uid IN {})); \n"
              .format(sql_trackers, sql_trackers))
    if anonimize_phone:
        # done delete PHONE_NUMBER
        write(f, "delete from trackedentityattributevalue where "
                 "trackedentityinstanceid in (select trackedentityattributeid from trackedentityattribute "
                 "where valuetype like 'PHONE_NUMBER') {}; \n".format(where))
        write(f,
              "UPDATE programstageinstance SET "
              "eventdatavalues = eventdatavalues - array(SELECT uid FROM   dataelement WHERE "
              " valuetype = 'PHONE_NUMBER' "
              "AND dataelementid IN"
              "(SELECT psde.dataelementid FROM   program p INNER JOIN programstage ps "
              "ON p.programid = ps.programid INNER JOIN programstagedataelement psde "
              "ON psde.programstageid = ps.programstageid WHERE "
              " p.uid IN {})) "
              "WHERE eventdatavalues ?| array(SELECT uid FROM   dataelement "
              "WHERE   valuetype = 'PHONE_NUMBER' "
              " AND dataelementid IN (SELECT psde.dataelementid"
              " FROM program p INNER JOIN programstage ps ON p.programid = ps.programid INNER JOIN "
              "programstagedataelement psde ON psde.programstageid = ps.programstageid WHERE  p.uid IN {})); \n"
              .format(sql_trackers, sql_trackers))
    if anonimize_mail:
        write(f, "---email\n")
        write(f,
              "UPDATE trackedentityattributevalue set value=('randomuser' || round(random()*"
              "trackedentityinstanceid+trackedentityattributeid)::text || {}) "
              "where trackedentityattributeid in (select trackedentityattributeid "
              "from trackedentityattribute where valuetype='EMAIL') {} ;\n".format(anonymize_email, where))
        write(f,
              "UPDATE programstageinstance SET "
              "eventdatavalues = eventdatavalues - array(SELECT uid FROM   dataelement WHERE "
              " valuetype = 'EMAIL' "
              "AND dataelementid IN"
              "(SELECT psde.dataelementid FROM   program p INNER JOIN programstage ps "
              "ON p.programid = ps.programid INNER JOIN programstagedataelement psde "
              "ON psde.programstageid = ps.programstageid WHERE "
              " p.uid IN {})) "
              "WHERE eventdatavalues ?| array(SELECT uid FROM   dataelement "
              "WHERE   valuetype = 'EMAIL' "
              " AND dataelementid IN (SELECT psde.dataelementid"
              " FROM program p INNER JOIN programstage ps ON p.programid = ps.programid INNER JOIN "
              "programstagedataelement psde ON psde.programstageid = ps.programstageid WHERE  p.uid IN {})); \n".format(
                  sql_trackers, sql_trackers))


def convert_to_sql_format(list_uid):
    if len(list_uid) == 0:
        return ""
    return "(" + ", ".join(["'{}'".format(uid) for uid in list_uid]) + ")"
