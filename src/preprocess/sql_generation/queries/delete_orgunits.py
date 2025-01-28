from src.preprocess.sql_generation.sql_common import write, convert_to_possible_paths_in_sql_format
from src.preprocess.sql_generation.versioned_table_names import *


def delete_org_unit_data_and_views(f):
    write(f, """
        --remove organisationUnits -- data
        DELETE FROM {programstageinstancecomments}     WHERE {programstageinstanceid}  IN (SELECT * FROM rm_programstageinstance);
    """.format(programstageinstancecomments= get_event_comment_table(), programstageinstanceid=get_event_identifier_name()))

    if Config.get_pre_api_version() <= 36:
        write(f, """
            DELETE FROM programinstanceaudit             WHERE {programinstanceid}       IN (SELECT * FROM rm_programinstance);
        """.format(programinstanceid=get_enrollment_identifier_name()))

    write(f, """
        DELETE FROM trackedentitydatavalueaudit      WHERE {programstageinstanceid}  IN (SELECT * FROM rm_programstageinstance);
        DELETE FROM {programstageinstance}             WHERE {programstageinstanceid}  IN (SELECT * FROM rm_programstageinstance);
        
        DELETE FROM {programinstancecomments}          WHERE {programinstanceid}       IN (SELECT * FROM rm_programinstance);
        DELETE FROM {programinstance}                  WHERE {programinstanceid}       IN (SELECT * FROM rm_programinstance);
        DELETE FROM datavalue where sourceid in (select organisationunitid from orgUnitsToDelete);
        DELETE FROM datavalueaudit where organisationunitid in (select organisationunitid  from orgUnitsToDelete);
        
        DELETE FROM trackedentityattributevalue      WHERE {trackedentityinstanceid} IN (SELECT * FROM rm_trackedentityinstance);
        DELETE FROM trackedentityattributevalueaudit WHERE {trackedentityinstanceid} IN (SELECT * FROM rm_trackedentityinstance);
        DELETE FROM trackedentityprogramowner        WHERE organisationunitid      IN (SELECT * FROM orgUnitsToDelete);
        DELETE FROM {trackedentityinstance}            WHERE {trackedentityinstanceid} IN (SELECT * FROM rm_trackedentityinstance);
    """.format(programstageinstanceid = get_event_identifier_name(), programstageinstance = get_event_table_name(),
                   programinstancecomments=get_event_comment_table(), programinstanceid= get_enrollment_identifier_name(),
                   programinstance=get_enrollment_table_name(), trackedentityinstanceid=get_tracker_identifier_name(),
                   trackedentityinstance=get_tracker_table_name()))
    write(f, """
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
        
        CREATE MATERIALIZED VIEW rm_trackedentityinstance 
            AS SELECT {trackedentityinstanceid} FROM {trackedentityinstance} WHERE 
                organisationunitid IN (SELECT * FROM orgUnitsToDelete); 
        CREATE UNIQUE INDEX idx_trackedentityinstance ON rm_trackedentityinstance ({trackedentityinstanceid}); 
        
        CREATE MATERIALIZED VIEW rm_programinstance 
            AS SELECT {programinstanceid} FROM {programinstance} WHERE 
                organisationunitid IN (SELECT * FROM orgUnitsToDelete); 
        CREATE UNIQUE INDEX idx_programinstance ON rm_programinstance ({programinstanceid}); 
        
        CREATE MATERIALIZED VIEW rm_programstageinstance_orgs 
            AS SELECT {programstageinstanceid} FROM {programstageinstance} WHERE 
                organisationunitid IN (SELECT * FROM orgUnitsToDelete); 
        CREATE MATERIALIZED VIEW rm_programstageinstance_programinstance 
            AS SELECT {programstageinstanceid} FROM {programstageinstance} WHERE 
                {programinstanceid} IN (SELECT * FROM rm_programinstance); 
        CREATE MATERIALIZED VIEW rm_programstageinstance 
            AS SELECT * FROM rm_programstageinstance_orgs 
            UNION ALL SELECT * FROM rm_programstageinstance_programinstance; 
        CREATE UNIQUE INDEX idx_programstageinstance ON rm_programstageinstance ({programstageinstanceid}); 
        
        CREATE MATERIALIZED VIEW rm_interpretation 
            AS SELECT interpretationid FROM interpretation WHERE organisationunitid IN (SELECT * FROM orgUnitsToDelete); 
        CREATE UNIQUE INDEX idx_interpretation ON rm_interpretation (interpretationid); 
        
        CREATE MATERIALIZED VIEW rm_programmessage 
            AS SELECT id FROM programmessage WHERE 
                organisationunitid      IN (SELECT * FROM orgUnitsToDelete) OR 
                {trackedentityinstanceid} IN (SELECT * FROM rm_trackedentityinstance) OR 
                {programstageinstanceid}  IN (SELECT * FROM rm_programstageinstance) OR 
                {programinstanceid}       IN (SELECT * FROM rm_programinstance); 
        CREATE UNIQUE INDEX idx_programmessage ON rm_programmessage ({id}); 
        CREATE INDEX IF NOT EXISTS idx_datavalue_organisationunitid                 ON datavalue                 (sourceid); 
        CREATE INDEX IF NOT EXISTS idx_datavalueaudit_organisationunitid            ON datavalueaudit            (organisationunitid); 
        CREATE INDEX IF NOT EXISTS idx_program_organisationunits_organisationunitid ON program_organisationunits (organisationunitid); 
        CREATE INDEX IF NOT EXISTS idx_orgunitgroup_organisationunitid              ON orgunitgroupmembers       (organisationunitid); 
        CREATE INDEX IF NOT EXISTS idx_programinstance_organisationunitid           ON {programinstance}           (organisationunitid); 
        CREATE INDEX IF NOT EXISTS idx_dataset_organisationunit                     ON datasetsource             (sourceid); 
        CREATE INDEX IF NOT EXISTS idx_parentid                                     ON organisationunit          (parentid); 
        CREATE INDEX IF NOT EXISTS idx_programstageinstance_organisationunitid      ON {programstageinstance}      (organisationunitid); 
        CREATE INDEX IF NOT EXISTS idx_trackedentityinstance_organisationunitid     ON {trackedentityinstance}     (organisationunitid); 
        CREATE INDEX IF NOT EXISTS idx_entityinstancedatavalueaudit_programstageinstanceid ON trackedentitydatavalueaudit              ({programstageinstanceid}); 
        CREATE INDEX IF NOT EXISTS idx_programmessage_programstageinstanceid               ON programmessage                           ({programstageinstanceid}); 
        CREATE INDEX IF NOT EXISTS idx_programstageinstancecomments_programstageinstanceid ON {programstageinstancecomments}             ({programstageinstanceid}); 
        CREATE INDEX IF NOT EXISTS idx_programstagenotification_psi                        ON programnotificationinstance              ({programstageinstanceid}); 
        CREATE INDEX IF NOT EXISTS idx_relationshipitem_programstageinstanceid             ON relationshipitem                         ({programstageinstanceid}); 
        CREATE INDEX IF NOT EXISTS idx_s9i10v8xg7d22hlhmesia51l                            ON programstageinstance_messageconversation ({programstageinstanceid}); 
    """).format(programstageinstanceid=get_event_identifier_name(), programstageinstance=get_event_table_name(),programinstanceid=get_enrollment_identifier_name(),programinstance=get_enrollment_table_name(),trackerentityinstanceid=get_tracker_identifier_name()
            ,trackerentityinstance=get_tracker_table_name())


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
