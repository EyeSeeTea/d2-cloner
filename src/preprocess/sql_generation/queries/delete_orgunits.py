from src.preprocess.sql_generation.sql_common import write, convert_to_possible_paths_in_sql_format
from src.preprocess.sql_generation.versioned_table_names import *


def delete_org_unit_data_and_views(f):
    write(f, """
        SELECT 'Starting DELETE block  for OU delete' AS d2_docker_presql_script;
        SELECT 'orgUnitsToDelete: ' || COUNT(*)::text AS d2_docker_presql_script FROM orgUnitsToDelete;
        SELECT 'rm_trackedentity: ' || COUNT(*)::text AS d2_docker_presql_script FROM rm_trackedentity;
        SELECT 'rm_enrollment: ' || COUNT(*)::text AS d2_docker_presql_script FROM rm_enrollment;
    """)
    for event_table in get_event_tables():
        write(f, "SELECT 'rm_event{suffix}: ' || COUNT(*)::text AS d2_docker_presql_script FROM rm_event{suffix};\n"
              .format(suffix=event_table.view_suffix))

    if Config.get_pre_api_version() <= 36:
        write(f, """
            DELETE FROM enrollmentaudit             WHERE {enrollmentid}       IN (SELECT * FROM rm_enrollment);
        """.format(enrollmentid=get_enrollment_identifier_name()))

    write(f, """
        {delete_events}
        
        DELETE FROM {enrollment_comment}          WHERE {enrollmentid}       IN (SELECT * FROM rm_enrollment);
        DELETE FROM {enrollment}                  WHERE {enrollmentid}       IN (SELECT * FROM rm_enrollment);
        DELETE FROM datavalue where sourceid in (select organisationunitid from orgUnitsToDelete);
        DELETE FROM datavalueaudit where organisationunitid in (select organisationunitid  from orgUnitsToDelete);
        
        DELETE FROM trackedentityattributevalue      WHERE {trackedentityid} IN (SELECT * FROM rm_trackedentity);
        DELETE FROM trackedentityattributevalueaudit WHERE {trackedentityid} IN (SELECT * FROM rm_trackedentity);
        DELETE FROM trackedentityprogramowner        WHERE organisationunitid      IN (SELECT * FROM orgUnitsToDelete);
        DELETE FROM {trackedentity}            WHERE {trackedentityid} IN (SELECT * FROM rm_trackedentity);
    """.format(delete_events="\n        ".join("""
        DELETE FROM trackedentitydatavalueaudit      WHERE {eventid}  IN (SELECT * FROM rm_event{suffix});
        DELETE FROM {event_comment}     WHERE {eventid}  IN (SELECT * FROM rm_event{suffix});
        DELETE FROM {event}             WHERE {eventid}  IN (SELECT * FROM rm_event{suffix});""".format(
                       eventid=et.identifier, event=et.table, event_comment=et.comment_table, suffix=et.view_suffix)
                       for et in get_event_tables()),
                   enrollmentid= get_enrollment_identifier_name(),
                   enrollment=get_enrollment_table_name(), trackedentityid=get_tracker_identifier_name(),
                   enrollment_comment=get_enrollment_comment_table(),
                   trackedentity=get_tracker_table_name()))

    if Config.get_pre_api_version() <= 38:
        write(f, """
            DELETE FROM interpretationuseraccesses       WHERE interpretationid        IN (SELECT * FROM rm_interpretation);
            DELETE FROM organisationunitattributevalues where organisationunitid in (select * from orgUnitsToDelete);
            DELETE FROM chart_organisationunits where organisationunitid in (select * from orgUnitsToDelete);
            """)

    write(f, """
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
        DELETE FROM dataapproval where organisationunitid in (select * from orgUnitsToDelete);
        DELETE FROM dataapprovalaudit where organisationunitid in (select * from orgUnitsToDelete);
        DELETE FROM eventchart_organisationunits where organisationunitid in (select * from orgUnitsToDelete);
        DELETE FROM eventreport_organisationunits where organisationunitid in (select * from orgUnitsToDelete);
        DELETE FROM eventvisualization_organisationunits WHERE organisationunitid      IN (SELECT * FROM orgUnitsToDelete);
        DELETE FROM lockexception where organisationunitid in (select * from orgUnitsToDelete);
        DELETE FROM mapview_organisationunits where organisationunitid in (select * from orgUnitsToDelete);
        DELETE FROM program_organisationunits where organisationunitid in (select * from orgUnitsToDelete);
        DELETE FROM programmessage_emailaddresses   WHERE programmessageemailaddressid      IN (SELECT * FROM rm_programmessage);
        DELETE FROM programmessage_deliverychannels  WHERE programmessagedeliverychannelsid IN (SELECT * FROM rm_programmessage);
        DELETE FROM programmessage                   WHERE id                               IN (SELECT * FROM rm_programmessage);
    """)
    if Config.get_pre_api_version() <= 38:
        write(f, """
            DELETE FROM reporttable_organisationunits where organisationunitid in (select * from orgUnitsToDelete);
        """)

    write(f, """
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

    write(f, f"""
    SELECT 'Close DELETE organisationunits Block' AS D2_DOCKER_PRESQL_SCRIPT;
    """)



def delete_org_units(f):
    create_org_units_to_remove_views_and_indexes(f)
    delete_org_unit_data_and_views(f)


def start_ou_materialized_view(f):
    write(f, """
        --remove organisationUnits -- org unit
        DROP MATERIALIZED VIEW if exists orgUnitsToDelete CASCADE;
    """)

    write(f, """
        --remove organisationUnits -- org unit
        CREATE MATERIALIZED VIEW orgUnitsToDelete AS (select distinct organisationunitid from organisationunit where \n
    """)


def create_org_units_to_remove_views_and_indexes(f):
    write(f, """
        CREATE UNIQUE INDEX idx_orgs ON orgUnitsToDelete (organisationunitid); 
        
        CREATE MATERIALIZED VIEW rm_trackedentity 
            AS SELECT {trackedentityid} FROM {trackedentity} WHERE 
                organisationunitid IN (SELECT * FROM orgUnitsToDelete); 
        CREATE UNIQUE INDEX idx_trackedentity ON rm_trackedentity ({trackedentityid}); 
        
        CREATE MATERIALIZED VIEW rm_enrollment 
            AS SELECT {enrollmentid} FROM {enrollment} WHERE 
                organisationunitid IN (SELECT * FROM orgUnitsToDelete); 
        CREATE UNIQUE INDEX idx_enrollment ON rm_enrollment ({enrollmentid}); 
        
        {create_event_views}
        
        CREATE MATERIALIZED VIEW rm_interpretation 
            AS SELECT interpretationid FROM interpretation WHERE organisationunitid IN (SELECT * FROM orgUnitsToDelete); 
        CREATE UNIQUE INDEX idx_interpretation ON rm_interpretation (interpretationid); 
        
        CREATE MATERIALIZED VIEW rm_programmessage 
            AS SELECT id FROM programmessage WHERE 
                organisationunitid      IN (SELECT * FROM orgUnitsToDelete) OR 
                {trackedentityid} IN (SELECT * FROM rm_trackedentity) OR 
                {programmessage_event_conditions}
                {enrollmentid}       IN (SELECT * FROM rm_enrollment); 
        CREATE UNIQUE INDEX idx_programmessage ON rm_programmessage (id); 
        CREATE INDEX IF NOT EXISTS idx_datavalue_organisationunitid                 ON datavalue                 (sourceid); 
        CREATE INDEX IF NOT EXISTS idx_datavalueaudit_organisationunitid            ON datavalueaudit            (organisationunitid); 
        CREATE INDEX IF NOT EXISTS idx_program_organisationunits_organisationunitid ON program_organisationunits (organisationunitid); 
        CREATE INDEX IF NOT EXISTS idx_orgunitgroup_organisationunitid              ON orgunitgroupmembers       (organisationunitid); 
        CREATE INDEX IF NOT EXISTS idx_enrollment_organisationunitid           ON {enrollment}           (organisationunitid); 
        CREATE INDEX IF NOT EXISTS idx_dataset_organisationunit                     ON datasetsource             (sourceid); 
        CREATE INDEX IF NOT EXISTS idx_parentid                                     ON organisationunit          (parentid); 
        {event_organisationunit_indexes}
        CREATE INDEX IF NOT EXISTS idx_trackedentity_organisationunitid     ON {trackedentity}     (organisationunitid); 
        CREATE INDEX IF NOT EXISTS idx_entityinstancedatavalueaudit_eventid ON trackedentitydatavalueaudit              ({eventid}); 
        {event_reference_indexes}
        CREATE INDEX IF NOT EXISTS temp_idx_teav_trackedentityid ON trackedentityattributevalue ({trackedentityid});
        CREATE INDEX IF NOT EXISTS temp_idx_teavaudit_trackedentityid ON trackedentityattributevalueaudit ({trackedentityid});
        CREATE INDEX IF NOT EXISTS temp_idx_teprogowner_organisationunitid ON trackedentityprogramowner (organisationunitid);
        CREATE INDEX IF NOT EXISTS temp_idx_interpretation_ouid ON interpretation (organisationunitid);
        CREATE INDEX IF NOT EXISTS temp_idx_categoryoption_ou_ouid ON categoryoption_organisationunits (organisationunitid);
        CREATE INDEX IF NOT EXISTS temp_idx_completedsreg_sourceid ON completedatasetregistration (sourceid);
        CREATE INDEX IF NOT EXISTS temp_idx_dataapproval_ouid ON dataapproval (organisationunitid);
        CREATE INDEX IF NOT EXISTS temp_idx_dataapprovalaudit_ouid ON dataapprovalaudit (organisationunitid);
        CREATE INDEX IF NOT EXISTS temp_idx_eventvis_ou_ouid ON eventvisualization_organisationunits (organisationunitid);
        CREATE INDEX IF NOT EXISTS temp_idx_lockexception_ouid ON lockexception (organisationunitid);
        CREATE INDEX IF NOT EXISTS temp_idx_mapview_ou_ouid ON mapview_organisationunits (organisationunitid);
        CREATE INDEX IF NOT EXISTS temp_idx_progmessage_ouid ON programmessage (organisationunitid);
        CREATE INDEX IF NOT EXISTS temp_idx_progownerhist_ouid ON programownershiphistory (organisationunitid);
        CREATE INDEX IF NOT EXISTS temp_idx_userdataviewou_ouid ON userdatavieworgunits (organisationunitid);
        CREATE INDEX IF NOT EXISTS temp_idx_usermembership_ouid ON usermembership (organisationunitid);
        CREATE INDEX IF NOT EXISTS temp_idx_userteisearchou_ouid ON userteisearchorgunits (organisationunitid);
        CREATE INDEX IF NOT EXISTS temp_idx_validationresult_ouid ON validationresult (organisationunitid);
        CREATE INDEX IF NOT EXISTS temp_idx_visualization_ou_ouid ON visualization_organisationunits (organisationunitid);
        ANALYZE orgUnitsToDelete;
        ANALYZE rm_trackedentity;
        ANALYZE rm_enrollment;
        {analyze_event_views}
        ANALYZE rm_interpretation;
        ANALYZE rm_programmessage;
    """.format(eventid=get_event_identifier_name(), enrollmentid=get_enrollment_identifier_name(),
               enrollment=get_enrollment_table_name(), trackedentity=get_tracker_table_name(), trackedentityid = get_tracker_identifier_name(),
               create_event_views=_create_event_views(), analyze_event_views=_analyze_event_views(),
               programmessage_event_conditions=" ".join(
                   "{} IN (SELECT * FROM rm_event{}) OR".format(et.reference_column, et.view_suffix)
                   for et in get_event_tables()),
               event_organisationunit_indexes="\n        ".join(
                   "CREATE INDEX IF NOT EXISTS idx_{event}_organisationunitid ON {event} (organisationunitid);".format(event=et.table)
                   for et in get_event_tables()),
               event_reference_indexes=_event_reference_indexes()))


def _create_event_views():
    views = []
    for et in get_event_tables():
        # Single events (DHIS2 >= 2.43) have no enrollment
        enrollment_view = """
        CREATE MATERIALIZED VIEW rm_event_enrollment{suffix} 
            AS SELECT {eventid} FROM {event} WHERE 
                {enrollmentid} IN (SELECT * FROM rm_enrollment); """ if et.has_enrollment else ""
        union_enrollment = "UNION ALL SELECT * FROM rm_event_enrollment{suffix}" if et.has_enrollment else ""
        views.append(("""
        CREATE MATERIALIZED VIEW rm_event_orgs{suffix} 
            AS SELECT {eventid} FROM {event} WHERE 
                organisationunitid IN (SELECT * FROM orgUnitsToDelete); """ + enrollment_view + """
        CREATE MATERIALIZED VIEW rm_event{suffix} 
            AS SELECT * FROM rm_event_orgs{suffix} {union_enrollment}; 
        CREATE UNIQUE INDEX idx_event{suffix} ON rm_event{suffix} ({eventid}); """).format(
            suffix=et.view_suffix, eventid=et.identifier, event=et.table, union_enrollment=union_enrollment.format(suffix=et.view_suffix),
            enrollmentid=get_enrollment_identifier_name()))
    return "\n".join(views)


def _analyze_event_views():
    lines = []
    for et in get_event_tables():
        lines.append("ANALYZE rm_event_orgs{0};".format(et.view_suffix))
        if et.has_enrollment:
            lines.append("ANALYZE rm_event_enrollment{0};".format(et.view_suffix))
        lines.append("ANALYZE rm_event{0};".format(et.view_suffix))
    return "\n        ".join(lines)


def _event_reference_indexes():
    lines = []
    for et in get_event_tables():
        lines.append("""
        CREATE INDEX IF NOT EXISTS idx_programmessage_{ref} ON programmessage ({ref});
        CREATE INDEX IF NOT EXISTS idx_event_comment_{table} ON {comment} ({eventid});
        CREATE INDEX IF NOT EXISTS idx_programnotificationinstance_{ref} ON programnotificationinstance ({ref});
        CREATE INDEX IF NOT EXISTS idx_relationshipitem_{ref} ON relationshipitem ({ref});""".format(
            ref=et.reference_column, table=et.table, comment=et.comment_table, eventid=et.identifier))
    if Config.get_pre_api_version() < 43:
        lines.append("CREATE INDEX IF NOT EXISTS idx_s9i10v8xg7d22hlhmesia51l ON event_messageconversation ({});".format(
            get_event_identifier_name()))
    return "\n".join(lines)


def generate_delete_org_unit_tree_rules(orgunits, f):
    path_query = ""
    for org_unit in orgunits:
        path_query = " (path like '%{}%' and uid <> '{}') or ".format(
            org_unit, org_unit)
    path_query = path_query[:-3]

    write(f,
          " (  {}  )\n".format(
              path_query))


def generate_delete_org_unit_level_by_parent_rules(level, parent_org_unit, f):
    write(f, """ ( hierarchylevel > {level} {parent} ) \n
            """.format(level=level, parent=convert_to_possible_paths_in_sql_format(parent_org_unit)))


def generate_delete_org_unit_level_rules(level, f):
    write(f, """ ( hierarchylevel > {level} ) \n """.format(level=level))
