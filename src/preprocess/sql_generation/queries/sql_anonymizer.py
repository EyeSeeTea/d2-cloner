from src.preprocess.sql_generation.sql_common import write, convert_to_sql_format
from src.preprocess.sql_generation.versioned_table_names import *

anonymize_email = "'@example.com'"


def generate_anonymize_user_queries(new_admin, old_admin, exclude_users, f, preprocess_api_version):
    write(f, """--anonimize users \n""")
    exclude_users_query = ""
    if len(exclude_users) > 0:
        exclude_users_query = """ 
            username not in {exclude_users} and 
        """.format(exclude_users=convert_to_sql_format(exclude_users))

    write(f, """ 
         DELETE FROM userrolemembers where userid=(select {userid} from {users} where username = '{new_admin}'); \n
     """.format(userid=get_user_identifier_name(),users=get_user_table_name(),new_admin=new_admin))
    write(f, """ 
        update userrolemembers set userid=(select {userid} from {users} where username = '{new}' ) 
        where userid=(select {userid} from {users} where username = '{old}'); \n
    """ .format(userid=get_user_identifier_name(),users=get_user_table_name(),new=new_admin,old=old_admin))

    if Config.get_pre_api_version() == 36:
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

    elif preprocess_api_version == 34:
        write(f, """ 
            update users set restorecode='-', password ='-', restoretoken='-', 
            disabled='t',secret='-', ldapid='', openid='' where {exclude} username not like '{new}'; \n
        """.format(exclude=exclude_users_query, new=new_admin))
        write(f, """
             update userinfo  set surname='-',firstname='-',email='',phonenumber='',
             jobtitle='',introduction='',gender='',birthday=null,nationality='',employer='',
             education='',interests='',languages='',welcomemessage='',whatsapp='',
             skype='',facebookmessenger='',telegram='',twitter='',avatar=null, attributevalues='{empty}' 
             where userinfoid in 
             (select userid from users where ({exclude} username not like '{new}')); 
        """.format(empty="{}", exclude=exclude_users_query, new=new_admin))


def generate_anonymize_datasets_rules(dataset_uids, org_units, data_elements,
                                      anonimize_org_units, anonimize_phone, anonimize_mail,
                                      anonimize_coordinate, all_uid, f):
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
        where_orgunits = """ 
        and sourceid in (select organisationunitid from organisationuint where uid in {orgunits}) 
        """.format(orgunits=sql_org_units)
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
                                   anonimize_coordinate, all_uid, f):
    write(f, "--anonymize events\n")
    if event_program != "":
        sql_event_program = convert_to_sql_format(event_program)
    else:
        sql_event_program = convert_to_sql_format(all_uid)
    sql_data_elements = convert_to_sql_format(data_elements)

    if anonimize_mail:
        write(f, """
                    ---test!!!
             UPDATE {programstageinstance} SET 
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
        """.format(sql_event_program=sql_event_program, programstageinstance=get_event_table_name()))
    if anonimize_phone:
        write(f,"""
            UPDATE {programstageinstance} SET 
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
        """.format(programs=sql_event_program, programstageinstance=get_event_table_name()))
    if anonimize_coordinate:
        write(f, """
             update {programinstance} as rand set geometry=null where {programinstanceid} 
             in ( select psi.{programinstanceid}  from {programstageinstance} psi 
             inner join programstage ps on ps.programstageid=psi.programstageid 
             inner join program p on p.programid=ps.programid where p.uid in {program});
        """.format(program=sql_event_program, programstageinstance=get_event_table_name(),
                   programinstanceid=get_enrollment_identifier_name(),programinstance=get_enrollment_table_name()))
        write(f, """
            update {programstageinstance} as rand set geometry=null where {programstageinstanceid} 
            in ( select psi.{programstageinstanceid}  from {programstageinstance} psi 
            inner join programstage ps on ps.programstageid=psi.programstageid 
            inner join program p on p.programid=ps.programid 
            where p.uid in {program});
        """.format(program=sql_event_program, programstageinstance=get_event_table_name(),programstageinstanceid=get_event_identifier_name()))

    if anonimize_org_units:
        write(f, """
            update {programinstance} as rand set organisationunitid=(select organisationunitid  
            from organisationunit where length(path)=(select length(path) from organisationunit where rand.organisationunitid= organisationunitid) ORDER BY RANDOM() limit 1) where {programinstanceid}  
            in ( select psi.{programinstanceid}  from programstageinstance psi 
            inner join programstage ps on ps.programstageid=psi.programstageid 
            inner join program p on p.programid=ps.programid where p.uid in {program});
        """.format(program=sql_event_program, programstageinstance=get_event_table_name(),programinstanceid=get_enrollment_identifier_name(), programinstance=get_enrollment_table_name()))
        write(f, """
             update {programstageinstance} as rand set organisationunitid=
             (select organisationunitid from organisationunit where length(path)=(select length(path) from organisationunit where rand.organisationunitid= organisationunitid) ORDER BY RANDOM()
             limit 1) where {programstageinstanceid} 
             in ( select psi.{programstageinstanceid}  from {programstageinstance} psi 
             inner join programstage ps on ps.programstageid=psi.programstageid 
             inner join program p on p.programid=ps.programid 
             where p.uid in {program});
        """.format(program=sql_event_program, programstageinstance=get_event_table_name(),programstageinstanceid=get_event_identifier_name()))

    if sql_data_elements != "":
        if sql_data_elements != "":
            for uid in data_elements:
                write(f, """
                     UPDATE {programstageinstance} SET    eventdatavalues = jsonb_set(eventdatavalues, 
                     '{ {x] ,value}', concat('',concat(concat('"',
                     (select concat('Redacted:',round(random()*dataelementid+{programstageinstanceid}))
                     FROM   dataelement WHERE  uid = 'sPadHOO4SQY'),'"',''))::jsonb)
                     WHERE  {programstageinstanceid} in (select {programstageinstanceid} from {programstageinstance}
                     where  eventdatavalues ? (SELECT de.uid FROM   dataelement as de
                     where  ( de.valuetype = 'TEXT' OR de.valuetype = 'LONG_TEXT' )
                     AND de.dataelementid IN (SELECT psde.dataelementid
                     FROM program p INNER JOIN programstage ps ON p.programid = ps.programid INNER JOIN
                     programstagedataelement psde ON psde.programstageid = ps.programstageid WHERE  p.uid IN 
                     ('{uid}')) and de.uid = '{all_uids}'));
                """).format(uid=uid, all_uids=', '.join(all_uid), programstageinstance=get_event_table_name())


def generate_anonymize_tracker_rules(trackers, tracker_attribute_values, organisationunits, data_elements,
                                     anonimize_org_units, anonimize_phone, anonimize_mail,
                                     anonimize_coordinate, all_uid, f):
    write(f, "--anonymize trackers \n")
    if trackers != "":
        sql_trackers = convert_to_sql_format(trackers)
    else:
        sql_trackers = convert_to_sql_format(all_uid)
    sql_organisationunits = convert_to_sql_format(organisationunits)
    sql_tracker_entity_attributes = convert_to_sql_format(
        tracker_attribute_values)
    sql_data_elements = convert_to_sql_format(data_elements)

    where = """ 
         and {trackedentityinstanceid} 
         in ( select ps.{trackedentityinstanceid} 
         from {programinstance} ps 
         inner join program p on p.programid=ps.programid 
         where p.uid in {trackers}) 
    """.format(trackers=sql_trackers, programinstance=get_enrollment_table_name(), trackedentityinstanceid=get_tracker_identifier_name())

    if sql_data_elements != "":
        for uid in data_elements:
            write(f, """
                 UPDATE {programstageinstance} SET    eventdatavalues = jsonb_set(eventdatavalues, 
                 '{{uid},value}', concat('',concat(concat('"',(select concat('random',
                 round(random()*dataelementid+{programstageinstanceid}))
                 FROM   dataelement WHERE  uid = 'sPadHOO4SQY'),'"'),''))::jsonb)
                 WHERE  {programstageinstanceid} in (select {programstageinstanceid} from {programstageinstance}
                 where  eventdatavalues ? (SELECT de.uid FROM   dataelement as de
                 where  ( de.valuetype = 'TEXT' OR de.valuetype = 'LONG_TEXT' )
                 AND de.dataelementid IN (SELECT psde.dataelementid
                 FROM program p INNER JOIN programstage ps ON p.programid = ps.programid INNER JOIN
                 programstagedataelement psde ON psde.programstageid = ps.programstageid WHERE  p.uid IN {sql_trackers}) 
                 and de.uid = '{uid}'));
            """).format(uid=uid, sql_trackers=sql_trackers, programstageinstance=get_event_table_name())

    if sql_tracker_entity_attributes != "":
        write(f, """
             UPDATE trackedentityattributevalue set 
             value=('Redacted ' || round(random()*{trackedentityinstanceid}+trackedentityattributeid)::text) 
             where trackedentityattributeid in (select trackedentityattributeid 
             from trackedentityattribute where valuetype='TEXT' or valuetype='LONG_TEXT')
             and trackedentityattributeid in (select trackedentityattributeid 
             from trackedentityattribute where uid in {trackerentity_uids} );
        """.format(trackerentity_uids=sql_tracker_entity_attributes,trackedentityinstanceid=get_tracker_identifier_name()))

    if sql_organisationunits != "":
        where = where + """
             and organisationunitid 
             in (select organisationunitid  from organisationunit 
             where uid in {orgunits})  
        """.format(orgunits=sql_organisationunits)
    if anonimize_coordinate:
        write(f, """
             update {programinstance} as rand set geometry=null where {programinstanceid} 
             in ( select psi.{programinstanceid}  from {programstageinstance} psi 
             inner join programstage ps on ps.programstageid=psi.programstageid 
             inner join program p on p.programid=ps.programid where p.uid in {trackers});
        """.format(trackers=sql_trackers, programstageinstance=get_event_table_name(),programinstance=get_enrollment_table_name(),programinstanceid=get_enrollment_identifier_name()))
        write(f, """
             update {programstageinstance} as rand set geometry=null where {programstageinstanceid} 
             in ( select psi.{programstageinstanceid}  from {programstageinstance} psi 
             inner join programstage ps on ps.programstageid=psi.programstageid 
             inner join program p on p.programid=ps.programid 
             where p.uid in {trackers});
        """.format(trackers=sql_trackers, programstageinstance=get_event_table_name(),programstageinstanceid=get_event_identifier_name()))
        write(f, """
             update {trackedentityinstance} as rand set coordinates=null where {trackedentityinstanceid} 
             in ( select ps.{trackedentityinstanceid}  from {programinstance} ps 
             inner join program p on p.programid=ps.programid 
             where p.uid in {trackers});
        """.format(trackers=sql_trackers,trackedentityinstanceid=get_tracker_identifier_name(),programinstance=get_enrollment_table_name(),trackedentityinstance=get_tracker_table_name()))
        write(f, """
             UPDATE {programstageinstance} SET 
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
        """.format(trackers=sql_trackers, programstageinstance=get_event_table_name()))

        write(f, """
             DELETE FROM trackedentityattributevalue where 
             {trackedentityinstanceid} in (select trackedentityattributeid from trackedentityattribute 
             where valuetype like 'COORDINATE') {where};
         """.format(where=where,trackedentityinstanceid=get_tracker_identifier_name()))

    if anonimize_org_units:
        write(f, """
             update {programinstance} as rand set organisationunitid=(select organisationunitid 
             from organisationunit where length(path)=(select length(path) from organisationunit where 
             rand.organisationunitid= organisationunitid) ORDER BY RANDOM() limit 1) where {programinstanceid} 
             in ( select psi.{programinstanceid}  from {programstageinstance} psi 
             inner join programstage ps on ps.programstageid=psi.programstageid 
             inner join program p on p.programid=ps.programid where p.uid in {trackers});
        """.format(trackers=sql_trackers, programstageinstance=get_event_table_name(),programinstance=get_enrollment_table_name(),programinstanceid=get_enrollment_identifier_name()))
        write(f, """
             update {programstageinstance} as rand set organisationunitid=(select organisationunitid 
             from organisationunit where length(path)=(select length(path) from 
             organisationunit where rand.organisationunitid= organisationunitid) ORDER BY RANDOM()
             limit 1) where {programstageinstanceid} 
             in ( select psi.{programstageinstanceid}  from {programstageinstance} psi 
             inner join programstage ps on ps.programstageid=psi.programstageid 
             inner join program p on p.programid=ps.programid 
             where p.uid in {trackers});
        """.format(trackers=sql_trackers, programstageinstance=get_event_table_name(),programstageinstanceid=get_event_identifier_name()))
        write(f, """
         update {trackedentityinstance} as rand set organisationunitid=(select 
         organisationunitid from organisationunit where length(path)=(select length(path) from 
         organisationunit where rand.organisationunitid= organisationunitid) ORDER BY RANDOM() 
         limit 1) where {trackedentityinstanceid} 
         in ( select ps.{trackedentityinstanceid}  from programinstance ps 
         inner join program p on p.programid=ps.programid 
         where p.uid in {trackers});
    """.format(trackers=sql_trackers,trackedentityinstanceid=get_tracker_identifier_name(),trackedentityinstance=get_tracker_table_name()))
        write(f, """
             update trackedentityprogramowner as rand set organisationunitid=(select organisationunitid from 
             organisationunit where length(path)=(select length(path) from organisationunit where 
             rand.organisationunitid= organisationunitid) ORDER BY RANDOM() limit 1) 
             where {trackedentityinstanceid} in (select {trackedentityinstanceid} from 
             program where uid in {trackers});
        """.format(trackers=sql_trackers,trackedentityinstanceid=get_tracker_identifier_name()))
        write(f, """
             DELETE FROM trackedentityattributevalue where 
             {trackedentityinstanceid} in (select trackedentityattributeid from trackedentityattribute 
             where valuetype like 'ORGANISATION_UNIT') {where}; 
        """.format(where=where,trackedentityinstanceid=get_tracker_identifier_name()))
        write(f, """
             UPDATE {programstageinstance} SET
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
         """.format(trackers=sql_trackers, programstageinstance=get_event_table_name()))

    if anonimize_phone:
        write(f, """
             DELETE FROM trackedentityattributevalue where
             {trackedentityinstanceid} in (select trackedentityattributeid from trackedentityattribute
             where valuetype like 'PHONE_NUMBER') {where};
        """.format(where=where,trackedentityinstanceid=get_tracker_identifier_name()))
        write(f, """
             UPDATE {programstageinstance} SET
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
        """.format(sql_trackers=sql_trackers, programstageinstance=get_event_table_name()))

    if anonimize_mail:
        write(f, """
            ---email
             DELETE FROM trackedentityattributevalue where
             {trackedentityinstanceid} in (select trackedentityattributeid from trackedentityattribute
             where valuetype like 'EMAIL') {where};
        """.format(where=where,trackedentityinstanceid=get_tracker_identifier_name()))

        write(f, """
             UPDATE {programstageinstance} SET
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
        """.format(sql_trackers=sql_trackers, programstageinstance=get_event_table_name()))
