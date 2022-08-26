import json

anonymize_email = "'@example.com'"


def remove_all_unnecessary_dependencies(f, preprocess_api_version):
    if preprocess_api_version == "34":
        write(f, """
DELETE FROM programstageinstance_messageconversation;
DELETE FROM programinstancecomments;
DELETE FROM programinstanceaudit;
DELETE FROM datavalueaudit;
DELETE FROM trackedentitydatavalueaudit;
DELETE FROM trackedentityattributevalueaudit;
DELETE FROM programstageinstance_messageconversation;
DELETE FROM dataapprovalaudit;
DELETE FROM interpretation_comments;
DELETE FROM interpretationcomment;
DELETE FROM messageconversation_messages;
DELETE FROM messageconversation_usermessages;
DELETE FROM messageconversation;
    """)
    elif preprocess_api_version == "36":
        write(f, """
DELETE FROM programstageinstance_messageconversation;
DELETE FROM programinstancecomments;
DELETE FROM datavalueaudit;
DELETE FROM trackedentitydatavalueaudit;
DELETE FROM trackedentityattributevalueaudit;
DELETE FROM programstageinstance_messageconversation;
DELETE FROM dataapprovalaudit;
DELETE FROM interpretation_comments;
DELETE FROM interpretationcomment;
DELETE FROM interpretationusergroupaccesses;
DELETE FROM intepretation_likedby;
DELETE FROM messageconversation_messages;
DELETE FROM messageconversation_usermessages;
DELETE FROM messageconversation;
    """)


def generate_delete_event_rules(event_program, data_elements, org_units,
                                org_unit_descendants, all_uid, f, preprocess_api_version):
    write(f, "--remove events\n")
    sql_all = convert_to_sql_format(all_uid)
    sql_event_program = convert_to_sql_format(event_program)
    sql_data_elements = convert_to_sql_format(data_elements)
    sql_org_units = convert_to_sql_format(org_units)
    sql_org_unit_descendants = convert_to_sql_format(org_unit_descendants)
    has_rule = False
    if sql_event_program != "":
        has_rule = True
        sql_query = " DELETE FROM programstageinstance where programstageid in " \
                    " (select programstageid from programstage where programid in " \
                    " (select programid from program where uid in {})) and".format(event_program)
    else:
        sql_query = " DELETE FROM programstageinstance where programstageid in " \
                    " (select programstageid from programstage where programid in " \
                    " (select programid from program where uid in {})) and".format(sql_all)
    if sql_data_elements != "":
        has_rule = True
        sql_data_elements = sql_data_elements.replace("(", "").replace(")", "")
        sql_query = " update programstageinstance set eventdatavalues = eventdatavalues - {} " \
                    " where eventdatavalues ? {} and ".format(sql_data_elements, sql_data_elements)
        if sql_event_program != "":
            sql_query = sql_query + " programstageid in (select programstageid from programstage " \
                                    " where programid in (select programid from program where uid in {})) and".format(
                event_program)
        else:
            sql_query = sql_query + " programstageid in (select programstageid from programstage " \
                                    " where programid in (select programid from program where uid in {})) and".format(
                sql_all)
    if sql_org_units != "":
        has_rule = True
        sql_query = sql_query + " organisationunitid in (select organisationunitid from organisationunit " \
                                " where uid in {}) and".format(sql_org_units)
    if sql_org_unit_descendants != "":
        has_rule = True
        sql_query = sql_query + " organisationunitid in (select organisationunitid from organisationunit " \
                                " where path like (select concat(path,'/%') from organisationunit " \
                                " where uid in {})) and".format(sql_org_unit_descendants)

    if not has_rule:
        delete_all_event_programs(all_uid, f)
    else:
        sql_query = sql_query + ";"
        sql_query = sql_query.replace("and;", ";")
        write(f, sql_query + "\n")


def generate_delete_tracker_rules(trackers, data_elements, org_units, org_unit_descendants,
                                  all_uid, f, preprocess_api_version):
    write(f, "--remove trackers \n")
    sql_all = convert_to_sql_format(all_uid)
    sql_trackers = convert_to_sql_format(trackers)
    sql_data_elements = convert_to_sql_format(data_elements)
    sql_org_units = convert_to_sql_format(org_units)
    sql_org_unit_descendants = convert_to_sql_format(org_unit_descendants)

    has_rule = False
    sql_query = "DELETE FROM programstageinstance where programstageid in " \
                " (select programstageid from programstage where programid in " \
                " (select programid from program where uid in {})) ".format(sql_all)
    if sql_data_elements != "":
        has_rule = True
        sql_data_elements = sql_data_elements.replace("(", "").replace(")", "")
        sql_query = " update programstageinstance set eventdatavalues = eventdatavalues - {}  where eventdatavalues ? " \
                    " {} and ".format(sql_data_elements, sql_data_elements)

        if sql_trackers != "":
            sql_query = sql_query + " programstageid in (select programstageid from programstage where programid in " \
                                    " (select programid from program where uid in {})) and".format(sql_trackers)
        else:
            sql_query = sql_query + " programstageid in (select programstageid from programstage where programid in " \
                                    " (select programid from program where uid in {})) and".format(sql_all)

    elif sql_trackers != "":
        has_rule = True
        sql_query = sql_query + " and dataelementid in (select dataelementid from datasetelement where datasetid in " \
                                " ( select datasetid from dataset where uid in {})) ".format(sql_trackers)
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


def generate_anonymize_user_queries(new_admin, old_admin, exclude_users, f, preprocess_api_version):
    write(f, "--anonimize users" + "\n")
    exclude_users_query = ""
    if len(exclude_users) > 0:
        exclude_users_query = " username not in {} and ".format(convert_to_sql_format(exclude_users))

    write(f, " DELETE FROM userrolemembers where userid=(select userid from users where username = '{}'); \n".format(
        new_admin))
    write(f, " update userrolemembers set userid=(select userid from users where username = '{new}' ) "
             " where userid=(select userid from users where username = '{old}'); \n".format(new=new_admin,
                                                                                            old=old_admin))
    if preprocess_api_version == "36":
        write(f, " update users set password ='-', restoretoken='-', "
                 " disabled='t',secret='-', ldapid=null, openid=null where {exclude} username not like '{new}'; \n".format(
            exclude=exclude_users_query,
            new=new_admin))
        write(f, """
     update userinfo  set surname='-',firstname='-',email='',phonenumber='',
     jobtitle='',introduction='',gender='',birthday=null,nationality='',employer='',
     education='',interests='',languages='',welcomemessage='',whatsapp='',
     skype='',facebookmessenger='',telegram='',twitter='',avatar=null, attributevalues='{empty}' 
     where userinfoid in 
     (select userid from users where ({exclude} username not like '{new}')); 
     """.format(empty="{}", exclude=exclude_users_query, new=new_admin))
    # userid | uid | code | created | lastupdated | creatoruserid | username |
    # password | externalauth | openid | ldapid | passwordlastupdated | lastlogin
    # | restoretoken | restoreexpiry | selfregistered | invitation | disabled
    # | lastupdatedby | secret | twofa | uuid | idtoken | accountexpiry
    elif preprocess_api_version == "34":
        write(f, " update users set restorecode='-', password ='-', restoretoken='-', "
                 " disabled='t',secret='-', ldapid='', openid='' where {exclude} username not like '{new}'; \n"
              .format(exclude=exclude_users_query, new=new_admin))
        write(f, """
     update userinfo  set surname='-',firstname='-',email='',phonenumber='',
     jobtitle='',introduction='',gender='',birthday=null,nationality='',employer='',
     education='',interests='',languages='',welcomemessage='',whatsapp='',
     skype='',facebookmessenger='',telegram='',twitter='',avatar=null, attributevalues='{empty}' 
     where userinfoid in 
     (select userid from users where ({exclude} username not like '{new}')); 
     """.format(empty="{}", exclude=exclude_users_query, new=new_admin))


def delete_org_unit_data_and_views(f):
    write(f, """
--remove organisationUnits -- data
DELETE FROM programstageinstancecomments     WHERE programstageinstanceid  IN (SELECT * FROM rm_programstageinstance);
DELETE FROM programinstanceaudit             WHERE programinstanceid       IN (SELECT * FROM rm_programinstance);
DELETE FROM trackedentitydatavalueaudit      WHERE programstageinstanceid  IN (SELECT * FROM rm_programstageinstance);
DELETE FROM programstageinstance             WHERE programstageinstanceid  IN (SELECT * FROM rm_programstageinstance);

DELETE FROM programinstancecomments          WHERE programinstanceid       IN (SELECT * FROM rm_programinstance);
DELETE FROM programinstance                  WHERE programinstanceid       IN (SELECT * FROM rm_programinstance);
DELETE FROM datavalue where sourceid in (select organisationunitid from orgUnitsToDelete);
DELETE FROM datavalueaudit where organisationunitid in (select organisationunitid  from orgUnitsToDelete);

DELETE FROM trackedentityattributevalue      WHERE trackedentityinstanceid IN (SELECT * FROM rm_trackedentityinstance);
DELETE FROM trackedentityattributevalueaudit WHERE trackedentityinstanceid IN (SELECT * FROM rm_trackedentityinstance);
DELETE FROM trackedentityprogramowner        WHERE organisationunitid      IN (SELECT * FROM orgs);
DELETE FROM trackedentityinstance            WHERE trackedentityinstanceid IN (SELECT * FROM rm_trackedentityinstance);

DELETE FROM interpretationuseraccesses       WHERE interpretationid        IN (SELECT * FROM rm_interpretation);
DELETE FROM interpretation_comments          WHERE interpretationid        IN (SELECT * FROM rm_interpretation);
DELETE FROM intepretation_likedby            WHERE interpretationid        IN (SELECT * FROM rm_interpretation);
DELETE FROM interpretation                   WHERE interpretationid        IN (SELECT * FROM rm_interpretation);

-- delete org unit, views and other dependencies

DELETE FROM datasetsource where sourceid in (select * from orgUnitsToDelete);
DELETE FROM orgunitgroupmembers where organisationunitid in (select * from orgUnitsToDelete);
DELETE FROM program_organisationunits where organisationunitid in (select * from orgUnitsToDelete);
DELETE FROM programownershiphistory              WHERE organisationunitid      IN (SELECT * FROM orgUnitsToDelete);
DELETE FROM _orgunitstructure where organisationunitid in (select * from orgUnitsToDelete);
DELETE FROM _datasetorganisationunitcategory where organisationunitid in (select * from orgUnitsToDelete);
DELETE FROM _organisationunitgroupsetstructure where organisationunitid in (select * from orgUnitsToDelete);
DELETE FROM datavalueaudit where organisationunitid in (select * from orgUnitsToDelete);
DELETE FROM categoryoption_organisationunits where organisationunitid in (select * from orgUnitsToDelete);
DELETE FROM chart_organisationunits where organisationunitid in (select * from orgUnitsToDelete);
DELETE FROM dataapproval where organisationunitid in (select * from orgUnitsToDelete);
DELETE FROM dataapprovalaudit where organisationunitid in (select * from orgUnitsToDelete);
DELETE FROM eventchart_organisationunits where organisationunitid in (select * from orgUnitsToDelete);
DELETE FROM eventreport_organisationunits where organisationunitid in (select * from orgUnitsToDelete);
DELETE FROM eventvisualization_organisationunits WHERE organisationunitid      IN (SELECT * FROM orgUnitsToDelete);
DELETE FROM lockexception where organisationunitid in (select * from orgUnitsToDelete);
DELETE FROM mapview_organisationunits where organisationunitid in (select * from orgUnitsToDelete);
DELETE FROM organisationunitattributevalues where organisationunitid in (select * from orgUnitsToDelete);
DELETE FROM program_organisationunits where organisationunitid in (select * from orgUnitsToDelete);
DELETE FROM programmessage_emailaddresses   WHERE programmessageemailaddressid   IN (SELECT * FROM rm_programmessage);
DELETE FROM programmessage_deliverychannels  WHERE programmessagedeliverychannelsid IN (SELECT * FROM rm_programmessage);
DELETE FROM programmessage                   WHERE id                               IN (SELECT * FROM rm_programmessage);
DELETE FROM reporttable_organisationunits where organisationunitid in (select * from orgUnitsToDelete);
DELETE FROM userdatavieworgunits where organisationunitid in (select * from orgUnitsToDelete);
DELETE FROM usermembership where organisationunitid in (select * from orgUnitsToDelete);
DELETE FROM userteisearchorgunits where organisationunitid in (select * from orgUnitsToDelete);
DELETE FROM validationresult where organisationunitid in (select * from orgUnitsToDelete);
DELETE FROM completedatasetregistration where sourceid in (select * from orgUnitsToDelete);
DELETE FROM configuration WHERE selfregistrationorgunit in (select * from orgUnitsToDelete);
DELETE FROM minmaxdataelement WHERE sourceid in (select * from orgUnitsToDelete);
DELETE FROM visualization_organisationunits WHERE organisationunitid in (select * from orgUnitsToDelete);
DELETE FROM organisationunit WHERE organisationunitid in (select * from orgUnitsToDelete);
DROP MATERIALIZED VIEW if exists orgUnitsToDelete CASCADE;
    
    """)


def generate_delete_org_unit_level_rules(level, f):
    write(f, """
--remove organisationUnits -- org unit
DROP MATERIALIZED VIEW if exists orgUnitsToDelete CASCADE;
CREATE MATERIALIZED VIEW orgUnitsToDelete AS select organisationunitid from organisationunit where hierarchylevel > {level} ;\n
    """.format(level=level))
    create_org_units_to_remove_views_and_indexes(f)
    delete_org_unit_data_and_views(f)


def create_org_units_to_remove_views_and_indexes(f):
    write(f, """
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
    write(f, """
    --remove organisationUnits -- org unit
    DROP MATERIALIZED VIEW if exists orgUnitsToDelete CASCADE;
    """)
    path_query = ""
    for org_unit in orgunits:
        path_query = " (path like '%{}%' and uid <> '{}') or ".format(org_unit, org_unit)
    path_query = path_query[:-3]
    write(f,
          "CREATE MATERIALIZED VIEW orgUnitsToDelete AS select distinct organisationunitid from organisationunit where {} ;\n".format(
              path_query))
    create_org_units_to_remove_views_and_indexes(f)
    delete_org_unit_data_and_views(f)


def generate_delete_datasets_rules(datasets, data_elements, org_units,
                                   org_unit_descendants, all_uid, f, preprocess_api_version):
    write(f, "--remove datasets" + "\n")
    sql_all = convert_to_sql_format(all_uid)
    sql_datasets = convert_to_sql_format(datasets)
    sql_data_elements = convert_to_sql_format(data_elements)
    sql_org_units = convert_to_sql_format(org_units)
    sql_org_unit_descendants = convert_to_sql_format(org_unit_descendants)
    sql_query = """
 DELETE FROM datavalue where dataelementid in (select dataelementid from datasetelement 
 where datasetid in ( select datasetid from dataset where uid in {all})) and""".format(all=sql_all)
    has_rule = False
    if sql_data_elements != "":
        has_rule = True
        if sql_datasets != "":
            sql_query = sql_query + """ 
 dataelementid in (select dataelementid from dataelement where uid in {dataelements} 
 and dataelementid in (select dataelementid from datasetelement where datasetid 
 in ( select datasetid from dataset where uid in {datasets})))
 and
 """.format(dataelements=sql_data_elements, datasets=sql_datasets)
        else:
            sql_query = sql_query + """ 
 dataelementid in (select dataelementid from dataelement where uid in {dataelements} 
 and dataelementid in (select dataelementid from datasetelement 
 where datasetid in ( select datasetid from dataset where uid in {datasets})))
 and
 """.format(dataelements=sql_data_elements, datasets=sql_all)
    elif sql_datasets != "":
        has_rule = True
        sql_query = sql_query + """ 
 dataelementid in (select dataelementid from datasetelement
 where datasetid in ( select datasetid from dataset where uid in {datasets}))
 and""".format(datasets=sql_datasets)
    if sql_org_units != "":
        has_rule = True
        sql_query = sql_query + " sourceid in (select organisationunitid from organisationunit where uid in {orgunits}) " \
                                "and".format(orgunits=sql_org_units)
    if sql_org_unit_descendants != "":
        has_rule = True
        sql_query = sql_query + """ sourceid in (select organisationunitid from organisationunit where path 
        like (select concat(path,'/%') from organisationunit where uid in {oudescendants})) 
        and""".format(oudescendants=sql_org_unit_descendants)

    if not has_rule:
        delete_all_data_sets(all_uid, f)
    else:
        sql_query = sql_query + ";"
        sql_query = sql_query.replace("and;", ";")
        write(f, sql_query + "\n")


def delete_all_event_programs(programs, f, preprocess_api_version):
    programs = convert_to_sql_format(programs)
    write(f, """
--remove all events
DELETE FROM trackedentitydatavalueaudit where programstageinstanceid 
in ( select psi.programstageinstanceid  from programstageinstance psi 
inner join programstage ps on ps.programstageid=psi.programstageid 
inner join program p on p.programid=ps.programid where p.uid in {programs});
""".format(programs=programs))
    write(f,
          """
        DELETE FROM programstageinstancecomments where programstageinstanceid 
        in ( select psi.programstageinstanceid  from programstageinstance psi 
        inner join programstage ps on ps.programstageid=psi.programstageid 
        inner join program p on p.programid=ps.programid where p.uid in {programs});
        """.format(programs=programs))
    write(f, """
DELETE FROM programstageinstance where programstageinstanceid 
in ( select psi.programstageinstanceid  from programstageinstance psi 
inner join programstage ps on ps.programstageid=psi.programstageid 
inner join program p on p.programid=ps.programid 
where p.uid in {programs});
""".format(programs=programs))


def delete_all_data_sets(datasets, f, preprocess_api_version):
    datasets = convert_to_sql_format(datasets)
    write(f, """
--remove all datasets
DELETE FROM datavalueaudit where dataelementid in 
(select dataelementid from datasetelement 
where datasetid in (select datasetid from dataset where uid in {datasets}));
""".format(datasets=datasets))
    write(f, """
DELETE FROM datavalue where dataelementid in (select dataelementid from datasetelement 
where datasetid in (select datasetid from dataset where uid in {datasets}));
""".format(datasets=datasets))
    pass


def delete_all_tracker_programs(trackers, f, preprocess_api_version):
    trackers = convert_to_sql_format(trackers)
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

    write(f, """
create view tei_to_remove as select trackedentityinstanceid "teiid"
from programinstance where programid in (select programid from program where uid in {trackers});
""".format(trackers=trackers))
    write(f,
          "DELETE FROM programinstance where programid in (select programid from program where uid in {trackers});\n".format(
              trackers=trackers))
    write(f, """DELETE FROM programinstance where trackedentityinstanceid in ( select * from tei_to_remove);
DELETE FROM trackedentityinstance where trackedentityinstanceid in ( select * from tei_to_remove);
DELETE FROM trackedentityprogramowner where trackedentityinstanceid in ( select * from tei_to_remove);
drop view tei_to_remove ;
--remove tracker finish
""")


def write(f, text):
    print(text)
    f.write(text)


def generate_anonymize_datasets_rules(dataset_uids, org_units, data_elements,
                                      anonimize_org_units, anonimize_phone, anonimize_mail,
                                      anonimize_coordinate, all_uid, f, preprocess_api_version):
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

    where_datasets = """
SELECT dataelementid AS lookup FROM datasetelement WHERE datasetid IN ( 
SELECT datasetid FROM dataset WHERE uid IN {datasets} 
""".format(datasets=datasets)

    if sql_data_elements != "":
        where_dataelements = " and uid in {dataelements} ".format(
            dataelements=sql_data_elements)

        write(f, """
Update datavalue set value=concat(\'Redacted:\',round(random()*dataelementid+categoryoptioncomboid+sourceid))
where dataelementid in (select dataelementid from dataelement where valuetype='TEXT' or valuetype='LONG_TEXT')"
and dataelementid in (select dataelementid from dataelement where uid in {dataelements}); 
""".format(dataelements=sql_data_elements))

    if sql_org_units != "":
        where_orgunits = " and sourceid in (select organisationunitid from organisationuint where uid in {orgunits})".format(
            orgunits=sql_org_units)
    else:
        where_orgunits = ""

    where_clausules = where_datasets + where_dataelements + where_orgunits
    if anonimize_org_units:
        where = "SELECT * FROM ( {} )".format(where_clausules)
        sql_query = """
UPDATE datavalue 
SET value = compare.update,  comment='' 
FROM ( {where} ) as valid_dataelements 
CROSS JOIN LATERAL ( 
SELECT t1.dataelementid, t1.sourceid, t1.periodid, t1.categoryoptioncomboid, t1.attributeoptioncomboid, t1.actual, t2.update 
FROM (SELECT row_number() OVER () AS 
index, dataelementid, sourceid, periodid, categoryoptioncomboid, attributeoptioncomboid, value AS actual 
FROM datavalue t1 
WHERE t1.dataelementid = lookup 
) t1 JOIN (SELECT row_number() OVER (ORDER BY random()) AS index, value AS update FROM datavalue t2 WHERE t2.dataelementid = lookup ) t2 USING (index) 
) AS lookup ) compare WHERE datavalue.dataelementid = compare.dataelementid AND datavalue.sourceid = compare.sourceid AND 
datavalue.periodid = compare.periodid AND 
datavalue.categoryoptioncomboid = compare.categoryoptioncomboid AND 
datavalue.attributeoptioncomboid = compare.attributeoptioncomboid;
""".format(where=where)
        write(f, sql_query + "\n")
    else:
        sql_query = """
update datavalue set comment = '' where 
dataelementid in (select dataelementid from datasetelement where datasetid 
in (select datasetid from dataset where uid in {datasets}));
""".format(datasets=datasets)
        write(f, sql_query + "\n")

    if anonimize_phone:
        write(f, """
Update datavalue set value=concat('+',round(random()*dataelementid+categoryoptioncomboid+sourceid))
where dataelementid in (select dataelementid from dataelement where valuetype='PHONE_NUMBER')
and dataelementid in ({where})); 
""".format(where=where_clausules))
    if anonimize_mail:
        write(f, """
Update datavalue set value=concat('user',round(random()*dataelementid+categoryoptioncomboid+sourceid)) || '@example.com' 
where dataelementid in (select dataelementid from dataelement where valuetype='EMAIL') 
and dataelementid in ({where}));
""".format(where=where_clausules))
    if anonimize_coordinate:
        write(f, """
DELETE FROM datavalue where dataelementid in (select dataelementid from dataelement where valuetype='COORDINATE') 
and dataelementid in ({where})); 
""".format(where=where_clausules))
    if anonimize_org_units:
        write(f, """
DELETE FROM datavalue where dataelementid in (select dataelementid from dataelement where valuetype='ORGANISATION_UNIT')
and dataelementid in ({where})); 
""".format(where=where_clausules))


def generate_anonymize_event_rules(event_program, organisationunits, data_elements,
                                   anonimize_org_units, anonimize_phone, anonimize_mail,
                                   anonimize_coordinate, all_uid, f, preprocess_api_version):
    write(f, "--anonymize events\n")
    if event_program != "":
        sql_event_program = convert_to_sql_format(event_program)
    else:
        sql_event_program = convert_to_sql_format(all_uid)
    sql_data_elements = convert_to_sql_format(data_elements)

    if anonimize_mail:
        write(f, """
        ---test!!!
 UPDATE programstageinstance SET 
 eventdatavalues = eventdatavalues - array(SELECT uid FROM   dataelement WHERE 
 valuetype = 'EMAIL' 
 AND dataelementid IN
 (SELECT psde.dataelementid FROM   program p INNER JOIN programstage ps 
 ON p.programid = ps.programid INNER JOIN programstagedataelement psde 
 ON psde.programstageid = ps.programstageid WHERE 
 p.uid IN {sql_event_program})) 
 WHERE eventdatavalues ?| array(SELECT uid FROM   dataelement 
 WHERE   valuetype = 'PHONE_NUMBER' 
 AND dataelementid IN (SELECT psde.dataelementid 
 FROM program p INNER JOIN programstage ps ON p.programid = ps.programid INNER JOIN 
 programstagedataelement psde ON psde.programstageid = ps.programstageid WHERE  p.uid IN {sql_event_program}));
""".format(sql_event_program=sql_event_program))
    if anonimize_phone:
        write(f,
              """
UPDATE programstageinstance SET 
eventdatavalues = eventdatavalues - array(SELECT uid FROM   dataelement WHERE  
valuetype = 'PHONE_NUMBER'  
AND dataelementid IN 
(SELECT psde.dataelementid FROM   program p INNER JOIN programstage ps  
ON p.programid = ps.programid INNER JOIN programstagedataelement psde 
ON psde.programstageid = ps.programstageid WHERE 
p.uid IN {programs}))  
WHERE eventdatavalues ?| array(SELECT uid FROM   dataelement  
WHERE   valuetype = 'PHONE_NUMBER' 
AND dataelementid IN (SELECT psde.dataelementid 
FROM program p INNER JOIN programstage ps ON p.programid = ps.programid INNER JOIN 
programstagedataelement psde ON psde.programstageid = ps.programstageid WHERE  p.uid IN {programs}));
                 """.format(program=sql_event_program))
    if anonimize_coordinate:
        write(f, """
 update programinstance as rand set geometry=null where programinstanceid 
 in ( select psi.programinstanceid  from programstageinstance psi 
 inner join programstage ps on ps.programstageid=psi.programstageid 
 inner join program p on p.programid=ps.programid where p.uid in {program});
""".format(program=sql_event_program))
        write(f, """
 update programstageinstance as rand set geometry=null where programstageinstanceid 
 in ( select psi.programstageinstanceid  from programstageinstance psi 
 inner join programstage ps on ps.programstageid=psi.programstageid 
 inner join program p on p.programid=ps.programid 
 where p.uid in {program});
""".format(program=sql_event_program))
    if anonimize_org_units:
        write(f, """
 update programinstance as rand set organisationunitid=(select organisationunitid 
 from organisationunit where length(path)=(select length(path) from organisationunit where rand.organisationunitid= organisationunitid) ORDER BY RANDOM() limit 1) where programinstanceid 
 in ( select psi.programinstanceid  from programstageinstance psi 
 inner join programstage ps on ps.programstageid=psi.programstageid 
 inner join program p on p.programid=ps.programid where p.uid in {program});
""".format(program=sql_event_program))
        write(f, """
 update programstageinstance as rand set organisationunitid=
 (select organisationunitid from organisationunit where length(path)=(select length(path) from organisationunit where rand.organisationunitid= organisationunitid) ORDER BY RANDOM()
 limit 1) where programstageinstanceid 
 in ( select psi.programstageinstanceid  from programstageinstance psi 
 inner join programstage ps on ps.programstageid=psi.programstageid 
 inner join program p on p.programid=ps.programid 
 where p.uid in {program});
""".format(program=sql_event_program))

    if sql_data_elements != "":
        if sql_data_elements != "":
            for uid in data_elements:
                write(f, """
 UPDATE programstageinstance SET    eventdatavalues = jsonb_set(eventdatavalues, 
 '{ {x] ,value}', concat('',concat(concat('"',
 (select concat('Redacted:',round(random()*dataelementid+programstageinstanceid))
 FROM   dataelement WHERE  uid = 'sPadHOO4SQY'),'"',''))::jsonb)
 WHERE  programstageinstanceid in (select programstageinstanceid from programstageinstance
 where  eventdatavalues ? (SELECT de.uid FROM   dataelement as de
 where  ( de.valuetype = 'TEXT' OR de.valuetype = 'LONG_TEXT' )
 AND de.dataelementid IN (SELECT psde.dataelementid
 FROM program p INNER JOIN programstage ps ON p.programid = ps.programid INNER JOIN
 programstagedataelement psde ON psde.programstageid = ps.programstageid WHERE  p.uid IN 
 ('{uid}')) and de.uid = '{all_uids}'));
 """).format(uid=uid, y=', '.join(all_uids=all_uid))


def generate_anonymize_tracker_rules(trackers, tracker_attribute_values, organisationunits, data_elements,
                                     anonimize_org_units, anonimize_phone, anonimize_mail,
                                     anonimize_coordinate, all_uid, f, preprocess_api_version):
    write(f, "--anonymize trackers \n")
    if trackers != "":
        sql_trackers = convert_to_sql_format(trackers)
    else:
        sql_trackers = convert_to_sql_format(all_uid)
    sql_organisationunits = convert_to_sql_format(organisationunits)
    sql_tracker_entity_attributes = convert_to_sql_format(tracker_attribute_values)
    sql_data_elements = convert_to_sql_format(data_elements)

    where = """ 
 and trackedentityinstanceid 
 in ( select ps.trackedentityinstanceid 
 from programinstance ps 
 inner join program p on p.programid=ps.programid 
 where p.uid in {trackers}) 
""".format(trackers=sql_trackers)

    if sql_data_elements != "":
        for uid in data_elements:
            write(f, """
 UPDATE programstageinstance SET    eventdatavalues = jsonb_set(eventdatavalues, 
 '{{uid},value}', concat('',concat(concat('"',(select concat('random',
 round(random()*dataelementid+programstageinstanceid))
 FROM   dataelement WHERE  uid = 'sPadHOO4SQY'),'"'),''))::jsonb)
 WHERE  programstageinstanceid in (select programstageinstanceid from programstageinstance
 where  eventdatavalues ? (SELECT de.uid FROM   dataelement as de
 where  ( de.valuetype = 'TEXT' OR de.valuetype = 'LONG_TEXT' )
 AND de.dataelementid IN (SELECT psde.dataelementid
 FROM program p INNER JOIN programstage ps ON p.programid = ps.programid INNER JOIN
 programstagedataelement psde ON psde.programstageid = ps.programstageid WHERE  p.uid IN {sql_trackers}) 
 and de.uid = '{uid}'));
""").format(uid=uid, sql_trackers=sql_trackers)

    if sql_tracker_entity_attributes != "":
        write(f, """
 UPDATE trackedentityattributevalue set 
 value=('Redacted ' || round(random()*trackedentityinstanceid+trackedentityattributeid)::text) 
 where trackedentityattributeid in (select trackedentityattributeid 
 from trackedentityattribute where valuetype='TEXT' or valuetype='LONG_TEXT')
 and trackedentityattributeid in (select trackedentityattributeid 
 from trackedentityattribute where uid in {trackerentity} );
""".format(trackerentity=sql_tracker_entity_attributes))

    if sql_organisationunits != "":
        where = where + """
 and organisationunitid 
 in (select organisationunitid  from organisationunit 
 where uid in {orgunits})  
 """.format(orgunits=sql_organisationunits)
    if anonimize_coordinate:
        write(f, """
 update programinstance as rand set geometry=null where programinstanceid 
 in ( select psi.programinstanceid  from programstageinstance psi 
 inner join programstage ps on ps.programstageid=psi.programstageid 
 inner join program p on p.programid=ps.programid where p.uid in {trackers});
""".format(trackers=sql_trackers))
        write(f, """
 update programstageinstance as rand set geometry=null where programstageinstanceid 
 in ( select psi.programstageinstanceid  from programstageinstance psi 
 inner join programstage ps on ps.programstageid=psi.programstageid 
 inner join program p on p.programid=ps.programid 
 where p.uid in {trackers});
""".format(trackers=sql_trackers))
        write(f, """
 update trackedentityinstance as rand set coordinates=null where trackedentityinstanceid 
 in ( select ps.trackedentityinstanceid  from programinstance ps 
 inner join program p on p.programid=ps.programid 
 where p.uid in {trackers});
""".format(trackers=sql_trackers))
        write(f, """
 UPDATE programstageinstance SET 
 eventdatavalues = eventdatavalues - array(SELECT uid FROM   dataelement WHERE 
 valuetype = 'COORDINATES' AND dataelementid IN
 (SELECT psde.dataelementid FROM   program p INNER JOIN programstage ps 
 ON p.programid = ps.programid INNER JOIN programstagedataelement psde 
 ON psde.programstageid = ps.programstageid WHERE 
 p.uid IN {trackers})) 
 WHERE eventdatavalues ?| array(SELECT uid FROM   dataelement 
 WHERE   valuetype = 'COORDINATES' AND dataelementid 
 IN (SELECT psde.dataelementid
 FROM program p INNER JOIN programstage ps ON p.programid = ps.programid INNER JOIN 
 programstagedataelement psde ON psde.programstageid = ps.programstageid WHERE  p.uid IN {trackers})); 
""".format(trackers=sql_trackers))

        write(f, """
 DELETE FROM trackedentityattributevalue where 
 trackedentityinstanceid in (select trackedentityattributeid from trackedentityattribute 
 where valuetype like 'COORDINATE') {where};
 """.format(where=where))

    if anonimize_org_units:
        write(f, """
 update programinstance as rand set organisationunitid=(select organisationunitid 
 from organisationunit where length(path)=(select length(path) from organisationunit where 
 rand.organisationunitid= organisationunitid) ORDER BY RANDOM() limit 1) where programinstanceid 
 in ( select psi.programinstanceid  from programstageinstance psi 
 inner join programstage ps on ps.programstageid=psi.programstageid 
 inner join program p on p.programid=ps.programid where p.uid in {trackers});
""".format(trackers=sql_trackers))
        write(f, """
 update programstageinstance as rand set organisationunitid=(select organisationunitid 
 from organisationunit where length(path)=(select length(path) from 
 organisationunit where rand.organisationunitid= organisationunitid) ORDER BY RANDOM()
 limit 1) where programstageinstanceid 
 in ( select psi.programstageinstanceid  from programstageinstance psi 
 inner join programstage ps on ps.programstageid=psi.programstageid 
 inner join program p on p.programid=ps.programid 
 where p.uid in {trackers});
""".format(trackers=sql_trackers))
        write(f, """
 update trackedentityinstance as rand set organisationunitid=(select 
 organisationunitid from organisationunit where length(path)=(select length(path) from 
 organisationunit where rand.organisationunitid= organisationunitid) ORDER BY RANDOM() 
 limit 1) where trackedentityinstanceid 
 in ( select ps.trackedentityinstanceid  from programinstance ps 
 inner join program p on p.programid=ps.programid 
 where p.uid in {trackers});
""".format(trackers=sql_trackers))
        write(f, """
 update trackedentityprogramowner as rand set organisationunitid=(select organisationunitid from 
 organisationunit where length(path)=(select length(path) from organisationunit where 
 rand.organisationunitid= organisationunitid) ORDER BY RANDOM() limit 1) 
 where trackedentityinstanceid in (select trackedentityinstanceid from 
 program where uid in {trackers});
 """.format(trackers=sql_trackers))
        write(f, """
 DELETE FROM trackedentityattributevalue where 
 trackedentityinstanceid in (select trackedentityattributeid from trackedentityattribute 
 where valuetype like 'ORGANISATION_UNIT') {where}; 
""".format(where=where))
        write(f, """
 UPDATE programstageinstance SET
 eventdatavalues = eventdatavalues - array(SELECT uid FROM dataelement WHERE 
 valuetype = 'ORGANISATION_UNIT' 
 AND dataelementid IN
 (SELECT psde.dataelementid FROM program p INNER JOIN programstage ps 
 ON p.programid = ps.programid INNER JOIN programstagedataelement psde 
 ON psde.programstageid = ps.programstageid WHERE 
 p.uid IN {trackers}))
 WHERE eventdatavalues ?| array(SELECT uid FROM   dataelement
 WHERE   valuetype = 'ORGANISATION_UNIT'
 AND dataelementid IN (SELECT psde.dataelementid
 FROM program p INNER JOIN programstage ps ON p.programid = ps.programid INNER JOIN
 programstagedataelement psde ON psde.programstageid = ps.programstageid WHERE  p.uid IN {trackers}));
 """.format(trackers=sql_trackers))

    if anonimize_phone:
        write(f, """
 DELETE FROM trackedentityattributevalue where
 trackedentityinstanceid in (select trackedentityattributeid from trackedentityattribute
 where valuetype like 'PHONE_NUMBER') {where};
""".format(where=where))
        write(f, """
 UPDATE programstageinstance SET
 eventdatavalues = eventdatavalues - array(SELECT uid FROM   dataelement WHERE
 valuetype = 'PHONE_NUMBER'
 AND dataelementid IN
 (SELECT psde.dataelementid FROM   program p INNER JOIN programstage ps
 ON p.programid = ps.programid INNER JOIN programstagedataelement psde
 ON psde.programstageid = ps.programstageid WHERE
 p.uid IN {sql_trackers}))
 WHERE eventdatavalues ?| array(SELECT uid FROM   dataelement
 WHERE   valuetype = 'PHONE_NUMBER'
 AND dataelementid IN (SELECT psde.dataelementid
 FROM program p INNER JOIN programstage ps ON p.programid = ps.programid INNER JOIN
 programstagedataelement psde ON psde.programstageid = ps.programstageid WHERE  p.uid IN {sql_trackers}));
""".format(sql_trackers=sql_trackers))

    if anonimize_mail:
        write(f, """---email
 DELETE FROM trackedentityattributevalue where
 trackedentityinstanceid in (select trackedentityattributeid from trackedentityattribute
 where valuetype like 'EMAIL') {where};
""".format(where=where))

        write(f, """
 UPDATE programstageinstance SET
 eventdatavalues = eventdatavalues - array(SELECT uid FROM   dataelement WHERE
 valuetype = 'EMAIL'
 AND dataelementid IN
 (SELECT psde.dataelementid FROM   program p INNER JOIN programstage ps
 ON p.programid = ps.programid INNER JOIN programstagedataelement psde
 ON psde.programstageid = ps.programstageid WHERE
 p.uid IN {sql_trackers}))
 WHERE eventdatavalues ?| array(SELECT uid FROM   dataelement
 WHERE   valuetype = 'EMAIL'
 AND dataelementid IN (SELECT psde.dataelementid
 FROM program p INNER JOIN programstage ps ON p.programid = ps.programid INNER JOIN
 programstagedataelement psde ON psde.programstageid = ps.programstageid WHERE  p.uid IN {sql_trackers}));
""".format(sql_trackers=sql_trackers))


def convert_to_sql_format(list_uid):
    if len(list_uid) == 0:
        return ""
    return "(" + ", ".join(["'{}'".format(uid) for uid in list_uid]) + ")"
