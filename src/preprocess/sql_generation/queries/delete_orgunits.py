from src.preprocess.sql_generation.sql_common import write, convert_to_possible_paths_in_sql_format
from src.preprocess.sql_generation.versioned_table_names import *


def delete_org_unit_data_and_views(f):
    write(f, """
        SELECT 'Starting DELETE block  for OU delete' AS d2_docker_presql_script;
        SELECT 'orgUnitsToDelete: ' || COUNT(*)::text AS d2_docker_presql_script FROM orgUnitsToDelete;
        SELECT 'rm_trackedentity: ' || COUNT(*)::text AS d2_docker_presql_script FROM rm_trackedentity;
        SELECT 'rm_enrollment: ' || COUNT(*)::text AS d2_docker_presql_script FROM rm_enrollment;
        SELECT 'rm_event: ' || COUNT(*)::text AS d2_docker_presql_script FROM rm_event;
    """)

    if Config.get_pre_api_version() <= 36:
        write(f, """
            DELETE FROM enrollmentaudit             WHERE {enrollmentid}       IN (SELECT * FROM rm_enrollment);
        """.format(enrollmentid=get_enrollment_identifier_name()))

    write(f, """
        DELETE FROM trackedentitydatavalueaudit      WHERE {eventid}  IN (SELECT * FROM rm_event);
        DELETE FROM {event_comment}     WHERE {eventid}  IN (SELECT * FROM rm_event);
        DELETE FROM {event}             WHERE {eventid}  IN (SELECT * FROM rm_event);
        
        DELETE FROM {enrollment_comment}          WHERE {enrollmentid}       IN (SELECT * FROM rm_enrollment);
        DELETE FROM {enrollment}                  WHERE {enrollmentid}       IN (SELECT * FROM rm_enrollment);
        DELETE FROM datavalue where sourceid in (select organisationunitid from orgUnitsToDelete);
        DELETE FROM datavalueaudit where organisationunitid in (select organisationunitid  from orgUnitsToDelete);
        
        DELETE FROM trackedentityattributevalue      WHERE {trackedentityid} IN (SELECT * FROM rm_trackedentity);
        DELETE FROM trackedentityattributevalueaudit WHERE {trackedentityid} IN (SELECT * FROM rm_trackedentity);
        DELETE FROM trackedentityprogramowner        WHERE organisationunitid      IN (SELECT * FROM orgUnitsToDelete);
        DELETE FROM {trackedentity}            WHERE {trackedentityid} IN (SELECT * FROM rm_trackedentity);
    """.format(eventid = get_event_identifier_name(), event = get_event_table_name(),
                   event_comment=get_event_comment_table(), enrollmentid= get_enrollment_identifier_name(),
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
    # Fail if residual materialized views exist from a previous failed execution.
    write(f, """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_matviews WHERE matviewname IN ('orgunitstodelete', 'rm_trackedentity', 'rm_enrollment', 'rm_event_orgs', 'rm_event_enrollment', 'rm_event', 'rm_interpretation', 'rm_programmessage')) THEN
                RAISE EXCEPTION 'Residual materialized views detected (orgUnitsToDelete, rm_trackedentity, etc). A previous deletion execution failed before cleanup. The database may contain data that should have been deleted. Manual intervention is required.';
            END IF;
        END
        $$;
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
        
        CREATE MATERIALIZED VIEW rm_event_orgs 
            AS SELECT {eventid} FROM {event} WHERE 
                organisationunitid IN (SELECT * FROM orgUnitsToDelete); 
        CREATE MATERIALIZED VIEW rm_event_enrollment 
            AS SELECT {eventid} FROM {event} WHERE 
                {enrollmentid} IN (SELECT * FROM rm_enrollment); 
        CREATE MATERIALIZED VIEW rm_event 
            AS SELECT * FROM rm_event_orgs 
            UNION ALL SELECT * FROM rm_event_enrollment; 
        CREATE UNIQUE INDEX idx_event ON rm_event ({eventid}); 
        
        CREATE MATERIALIZED VIEW rm_interpretation 
            AS SELECT interpretationid FROM interpretation WHERE organisationunitid IN (SELECT * FROM orgUnitsToDelete); 
        CREATE UNIQUE INDEX idx_interpretation ON rm_interpretation (interpretationid); 
        
        CREATE MATERIALIZED VIEW rm_programmessage 
            AS SELECT id FROM programmessage WHERE 
                organisationunitid      IN (SELECT * FROM orgUnitsToDelete) OR 
                {trackedentityid} IN (SELECT * FROM rm_trackedentity) OR 
                {eventid}  IN (SELECT * FROM rm_event) OR 
                {enrollmentid}       IN (SELECT * FROM rm_enrollment); 
        CREATE UNIQUE INDEX idx_programmessage ON rm_programmessage (id); 
        CREATE INDEX IF NOT EXISTS idx_datavalue_organisationunitid                 ON datavalue                 (sourceid); 
        CREATE INDEX IF NOT EXISTS idx_datavalueaudit_organisationunitid            ON datavalueaudit            (organisationunitid); 
        CREATE INDEX IF NOT EXISTS idx_program_organisationunits_organisationunitid ON program_organisationunits (organisationunitid); 
        CREATE INDEX IF NOT EXISTS idx_orgunitgroup_organisationunitid              ON orgunitgroupmembers       (organisationunitid); 
        CREATE INDEX IF NOT EXISTS idx_enrollment_organisationunitid           ON {enrollment}           (organisationunitid); 
        CREATE INDEX IF NOT EXISTS idx_dataset_organisationunit                     ON datasetsource             (sourceid); 
        CREATE INDEX IF NOT EXISTS idx_parentid                                     ON organisationunit          (parentid); 
        CREATE INDEX IF NOT EXISTS idx_event_organisationunitid      ON {event}      (organisationunitid); 
        CREATE INDEX IF NOT EXISTS idx_trackedentity_organisationunitid     ON {trackedentity}     (organisationunitid); 
        CREATE INDEX IF NOT EXISTS idx_entityinstancedatavalueaudit_eventid ON trackedentitydatavalueaudit              ({eventid}); 
        CREATE INDEX IF NOT EXISTS idx_programmessage_eventid               ON programmessage                           ({eventid}); 
        CREATE INDEX IF NOT EXISTS idx_event_comment_eventid ON {event_comment}             ({eventid}); 
        CREATE INDEX IF NOT EXISTS idx_programstagenotification_psi                        ON programnotificationinstance              ({eventid}); 
        CREATE INDEX IF NOT EXISTS idx_relationshipitem_eventid             ON relationshipitem                         ({eventid}); 
        CREATE INDEX IF NOT EXISTS idx_s9i10v8xg7d22hlhmesia51l                            ON event_messageconversation ({eventid}); 
    """.format(eventid=get_event_identifier_name(), event=get_event_table_name(),enrollmentid=get_enrollment_identifier_name(),
               enrollment=get_enrollment_table_name(), trackedentity=get_tracker_table_name(), trackedentityid = get_tracker_identifier_name(),
               event_comment=get_event_comment_table()))


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
