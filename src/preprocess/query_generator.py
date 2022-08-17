import json


def remove_all_unnecessary_dependencies(f):
    f.write("""

    delete from programstageinstance_messageconversation;
    delete from programinstancecomments;
    delete from programinstanceaudit;
    delete from datavalueaudit;
    delete from trackedentitydatavalueaudit;
    delete from trackedentityattributevalueaudit;
    delete from programstageinstance_messageconversation;
    delete from dataapprovalaudit;
    delete from interpretation_comments;
    delete from interpretationcomment;
    delete from interpretationusergroupaccesses;
    delete from intepretation_likedby;
    delete from messageconversation_messages;
    delete from messageconversation_usermessages;
    delete from messageconversation;
    drop view if exists _view_nhwa_data_audit;
    drop view if exists _view_test2;
    
    """)


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
                                "path like (select concat(path,'/%') from organisationunit where uid in {})) and".format(
            sql_org_unit_descendants)

    if not has_rule:
        delete_all_tracker_programs(all_uid, f)
    else:
        sql_query = sql_query + ";"
        sql_query = sql_query.replace("and;", ";")
        f.write(sql_query + "\n")


def delete_org_unit_data_in_view(f):
    f.write("""
        
        --remove organisationUnits -- data
        DELETE FROM programstageinstancecomments     WHERE programstageinstanceid  IN (SELECT * FROM rm_programstageinstance);
        DELETE FROM programinstanceaudit             WHERE programinstanceid       IN (SELECT * FROM rm_programinstance);
        DELETE FROM trackedentitydatavalueaudit      WHERE programstageinstanceid  IN (SELECT * FROM rm_programstageinstance);
        DELETE FROM programstageinstance             WHERE programstageinstanceid  IN (SELECT * FROM rm_programstageinstance);
        
        DELETE FROM programinstancecomments          WHERE programinstanceid       IN (SELECT * FROM rm_programinstance);
        DELETE FROM programinstance                  WHERE programinstanceid       IN (SELECT * FROM rm_programinstance);
        delete from datavalue where sourceid in (select organisationunitid from orgUnitsToDelete);
        delete from datavalueaudit where organisationunitid in (select organisationunitid  from orgUnitsToDelete);
        
        DELETE FROM trackedentityattributevalue      WHERE trackedentityinstanceid IN (SELECT * FROM rm_trackedentityinstance);
        DELETE FROM trackedentityattributevalueaudit WHERE trackedentityinstanceid IN (SELECT * FROM rm_trackedentityinstance);
        DELETE FROM trackedentityprogramowner        WHERE organisationunitid      IN (SELECT * FROM orgs);
        DELETE FROM trackedentityinstance            WHERE trackedentityinstanceid IN (SELECT * FROM rm_trackedentityinstance);
        
        DELETE FROM interpretationuseraccesses       WHERE interpretationid        IN (SELECT * FROM rm_interpretation);
        DELETE FROM interpretation_comments          WHERE interpretationid        IN (SELECT * FROM rm_interpretation);
        DELETE FROM intepretation_likedby            WHERE interpretationid        IN (SELECT * FROM rm_interpretation);
        DELETE FROM interpretation                   WHERE interpretationid        IN (SELECT * FROM rm_interpretation);
        """)


def delete_org_units_in_view(f):
    f.write("""
    
    DELETE FROM datasetsource where sourceid in (select * from orgUnitsToDelete);
    DELETE FROM orgunitgroupmembers where organisationunitid in (select * from orgUnitsToDelete);
    DELETE FROM program_organisationunits where organisationunitid in (select * from orgUnitsToDelete);
DELETE FROM programownershiphistory              WHERE organisationunitid      IN (SELECT * FROM orgUnitsToDelete);
    DELETE FROM  _orgunitstructure where organisationunitid in (select * from orgUnitsToDelete);
    DELETE FROM  _datasetorganisationunitcategory where organisationunitid in (select * from orgUnitsToDelete);
    DELETE FROM  _organisationunitgroupsetstructure where organisationunitid in (select * from orgUnitsToDelete);
    DELETE FROM  datavalueaudit where organisationunitid in (select * from orgUnitsToDelete);
    DELETE FROM  categoryoption_organisationunits where organisationunitid in (select * from orgUnitsToDelete);
    DELETE FROM  chart_organisationunits where organisationunitid in (select * from orgUnitsToDelete);
    DELETE FROM  dataapproval where organisationunitid in (select * from orgUnitsToDelete);
    DELETE FROM  dataapprovalaudit where organisationunitid in (select * from orgUnitsToDelete);
    DELETE FROM  eventchart_organisationunits where organisationunitid in (select * from orgUnitsToDelete);
    DELETE FROM  eventreport_organisationunits where organisationunitid in (select * from orgUnitsToDelete);
    DELETE FROM  interpretationuseraccesses where interpretationid in (select interpretationid from interpretation where organisationunitid in (select * from orgUnitsToDelete));
    DELETE FROM  interpretation_comments where interpretationid in (select interpretationid from interpretation where organisationunitid in (select * from orgUnitsToDelete));
    DELETE FROM  intepretation_likedby where interpretationid in (select interpretationid from interpretation where organisationunitid in (select * from orgUnitsToDelete));
    DELETE FROM  interpretationuseraccesses where interpretationid in (select interpretationid from interpretation where organisationunitid in (select * from orgUnitsToDelete));
    DELETE FROM  interpretation where organisationunitid in (select * from orgUnitsToDelete);
    DELETE FROM eventvisualization_organisationunits WHERE organisationunitid      IN (SELECT * FROM orgUnitsToDelete);
    DELETE FROM  lockexception where organisationunitid in (select * from orgUnitsToDelete);
    DELETE FROM  mapview_organisationunits where organisationunitid in (select * from orgUnitsToDelete);
    DELETE FROM  organisationunitattributevalues where organisationunitid in (select * from orgUnitsToDelete);
    DELETE FROM  program_organisationunits where organisationunitid in (select * from orgUnitsToDelete);
    DELETE FROM programmessage_emailaddresses    WHERE programmessageemailaddressid     IN (SELECT * FROM rm_programmessage);
    DELETE FROM programmessage_deliverychannels  WHERE programmessagedeliverychannelsid IN (SELECT * FROM rm_programmessage);
    DELETE FROM programmessage                   WHERE id                               IN (SELECT * FROM rm_programmessage);
    DELETE FROM  reporttable_organisationunits where organisationunitid in (select * from orgUnitsToDelete);
    DELETE FROM  userdatavieworgunits where organisationunitid in (select * from orgUnitsToDelete);
    DELETE FROM  usermembership where organisationunitid in (select * from orgUnitsToDelete);
    DELETE FROM  userteisearchorgunits where organisationunitid in (select * from orgUnitsToDelete);
    DELETE FROM  validationresult where organisationunitid in (select * from orgUnitsToDelete);
    DELETE FROM completedatasetregistration where sourceid in (select * from orgUnitsToDelete);
    DELETE FROM configuration WHERE selfregistrationorgunit in (select * from orgUnitsToDelete);
    DELETE FROM minmaxdataelement WHERE sourceid in (select * from orgUnitsToDelete);
    DELETE FROM visualization_organisationunits WHERE organisationunitid in (select * from orgUnitsToDelete);
    DELETE FROM organisationunit WHERE organisationunitid in (select * from orgUnitsToDelete);
    DROP MATERIALIZED VIEW if exists orgUnitsToDelete  CASCADE;
    
    """)


def generate_delete_org_unit_level_rules(level, f):
    f.write("""
    --remove organisationUnits -- org unit
    DROP MATERIALIZED VIEW if exists orgUnitsToDelete CASCADE;
    """)
    f.write(
        " CASCADE;"
        "CREATE MATERIALIZED VIEW orgUnitsToDelete AS select organisationunitid from organisationunit where hierarchylevel > {} ; \n".format(
            level))
    create_orgunittoberemoved_indixes(f)
    delete_org_unit_data_in_view(f)
    delete_org_units_in_view(f)


def create_orgunittoberemoved_indixes(f):
    f.write("""
        CREATE UNIQUE INDEX idx_orgs ON orgUnitsToDelete (organisationunitid); 

        CREATE MATERIALIZED VIEW rm_trackedentityinstance 
            AS SELECT trackedentityinstanceid FROM trackedentityinstance WHERE 
                organisationunitid IN (SELECT * FROM orgUnitsToDelete); 
        CREATE UNIQUE INDEX idx_trackedentityinstance ON rm_trackedentityinstance (trackedentityinstanceid); 

        CREATE MATERIALIZED VIEW rm_programinstance 
            AS SELECT programinstanceid FROM programinstance WHERE 
                organisationunitid IN (SELECT * FROM orgUnitsToDelete); 
        CREATE UNIQUE INDEX idx_programinstance ON rm_programinstance (programinstanceid); 

        CREATE MATERIALIZED VIEW rm_programstageinstance_orgs 
            AS SELECT programstageinstanceid FROM programstageinstance WHERE 
                organisationunitid IN (SELECT * FROM orgUnitsToDelete); 
        CREATE MATERIALIZED VIEW rm_programstageinstance_programinstance 
            AS SELECT programstageinstanceid FROM programstageinstance WHERE 
                programinstanceid IN (SELECT * FROM rm_programinstance); 
        CREATE MATERIALIZED VIEW rm_programstageinstance 
            AS SELECT * FROM rm_programstageinstance_orgs 
            UNION ALL SELECT * FROM rm_programstageinstance_programinstance; 
        CREATE UNIQUE INDEX idx_programstageinstance ON rm_programstageinstance (programstageinstanceid); 

        CREATE MATERIALIZED VIEW rm_interpretation 
            AS SELECT interpretationid FROM interpretation WHERE organisationunitid IN (SELECT * FROM orgUnitsToDelete); 
        CREATE UNIQUE INDEX idx_interpretation ON rm_interpretation (interpretationid); 
        
        CREATE MATERIALIZED VIEW rm_programmessage 
            AS SELECT id FROM programmessage WHERE 
                organisationunitid      IN (SELECT * FROM orgUnitsToDelete) OR 
                trackedentityinstanceid IN (SELECT * FROM rm_trackedentityinstance) OR 
                programstageinstanceid  IN (SELECT * FROM rm_programstageinstance) OR 
                programinstanceid       IN (SELECT * FROM rm_programinstance); 
        CREATE UNIQUE INDEX idx_programmessage ON rm_programmessage (id); 
        CREATE INDEX IF NOT EXISTS idx_datavalue_organisationunitid                 ON datavalue                 (sourceid); 
        CREATE INDEX IF NOT EXISTS idx_datavalueaudit_organisationunitid            ON datavalueaudit            (organisationunitid); 
        CREATE INDEX IF NOT EXISTS idx_program_organisationunits_organisationunitid ON program_organisationunits (organisationunitid); 
        CREATE INDEX IF NOT EXISTS idx_orgunitgroup_organisationunitid              ON orgunitgroupmembers       (organisationunitid); 
        CREATE INDEX IF NOT EXISTS idx_programinstance_organisationunitid           ON programinstance           (organisationunitid); 
        CREATE INDEX IF NOT EXISTS idx_dataset_organisationunit                     ON datasetsource             (sourceid); 
        CREATE INDEX IF NOT EXISTS idx_parentid                                     ON organisationunit          (parentid); 
        CREATE INDEX IF NOT EXISTS idx_programstageinstance_organisationunitid      ON programstageinstance      (organisationunitid); 
        CREATE INDEX IF NOT EXISTS idx_trackedentityinstance_organisationunitid     ON trackedentityinstance     (organisationunitid); 
        CREATE INDEX IF NOT EXISTS idx_entityinstancedatavalueaudit_programstageinstanceid ON trackedentitydatavalueaudit              (programstageinstanceid); 
        CREATE INDEX IF NOT EXISTS idx_programmessage_programstageinstanceid               ON programmessage                           (programstageinstanceid); 
        CREATE INDEX IF NOT EXISTS idx_programstageinstancecomments_programstageinstanceid ON programstageinstancecomments             (programstageinstanceid); 
        CREATE INDEX IF NOT EXISTS idx_programstagenotification_psi                        ON programnotificationinstance              (programstageinstanceid); 
        CREATE INDEX IF NOT EXISTS idx_relationshipitem_programstageinstanceid             ON relationshipitem                         (programstageinstanceid); 
        CREATE INDEX IF NOT EXISTS idx_s9i10v8xg7d22hlhmesia51l                            ON programstageinstance_messageconversation (programstageinstanceid); """)


def generate_delete_org_unit_tree_rules(orgunits, f):
    f.write("""
    --remove organisationUnits -- org unit
    DROP MATERIALIZED VIEW if exists orgUnitsToDelete CASCADE;
    """)
    path_query = ""
    for org_unit in orgunits:
        path_query = " (path like '%{}%' and uid <> '{}') or ".format(org_unit, org_unit)
    path_query = path_query[:-3]
    f.write(
        "CREATE MATERIALIZED VIEW orgUnitsToDelete AS select distinct organisationunitid from organisationunit where {} ;\n".format(
            path_query))
    create_orgunittoberemoved_indixes(f)
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
        "delete from programinstance where programid in (select programid from program where uid in {});\n".format(
            trackers))
    f.write("""
    
    delete from programinstance where trackedentityinstanceid in ( select * from tei_to_remove);
    delete from trackedentityinstance where trackedentityinstanceid in ( select * from tei_to_remove);
    delete from trackedentityprogramowner where trackedentityinstanceid in ( select * from tei_to_remove);
    drop view tei_to_remove ;
    --remove tracker finish
    
    """)


def convert_to_sql_format(list_uid):
    if len(list_uid) == 0:
        return ""
    return "(" + ", ".join(["'{}'".format(uid) for uid in list_uid]) + ")"
