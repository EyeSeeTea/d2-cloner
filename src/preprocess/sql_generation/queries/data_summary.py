from src.preprocess.sql_generation.sql_common import write
from src.preprocess.sql_generation.versioned_table_names import get_event_tables, get_enrollment_table_name, \
    get_enrollment_identifier_name, get_tracker_table_name, get_tracker_identifier_name

COUNTS_BEFORE_TABLE = "d2cloner_data_counts_before"
COUNTS_AFTER_TABLE = "d2cloner_data_counts_after"


def show_data_summary(f):
    count_datavalues_grouped_per_dataset(f)
    count_events_grouped_per_program(f)
    count_enrollments_grouped_per_program(f)
    count_tracked_entities_grouped_per_program(f)

def count_datavalues_grouped_per_dataset(f):
    write(f, "\n-- Data values per data set\n")
    write(f, "SELECT 'Database summary - data values per data set' AS D2_DOCKER_PRESQL_SCRIPT;\n")
    write(f, """
SELECT
    ds.uid   AS dataset_uid,
    ds.name  AS dataset_name,
    COUNT(dv.*) AS datavalue_count
FROM datavalue dv
JOIN dataelement de       ON dv.dataelementid = de.dataelementid
JOIN datasetelement dsm   ON dsm.dataelementid = de.dataelementid
JOIN dataset ds           ON ds.datasetid = dsm.datasetid
GROUP BY ds.uid, ds.name
ORDER BY ds.name DESC;
""")

def count_events_grouped_per_program(f):
    write(f, "\n-- Events per program\n")
    write(f, "SELECT 'Database summary - events per program' AS D2_DOCKER_PRESQL_SCRIPT;\n")
    write(f, """
SELECT
    p.uid AS program_uid,
    p.name AS program_name,
    COUNT(e.*) AS event_count
FROM ({events}) e
JOIN programstage ps ON e.programstageid = ps.programstageid
JOIN program p ON ps.programid = p.programid
GROUP BY p.uid, p.name
ORDER BY p.name DESC;
""".format(events=" UNION ALL ".join("SELECT programstageid FROM {}".format(et.table) for et in get_event_tables())))

def count_enrollments_grouped_per_program(f):
    write(f, "\n-- Enrollments per program\n")
    write(f, "SELECT 'Database summary - enrollments per program' AS D2_DOCKER_PRESQL_SCRIPT;\n")
    write(f, """
SELECT
    p.uid  AS program_uid,
    p.name AS program_name,
    COUNT(*) AS enrollment_count
FROM enrollment pi
JOIN program p ON p.programid = pi.programid
GROUP BY p.uid, p.name
ORDER BY p.name DESC;
""")

def count_tracked_entities_grouped_per_program(f):
    write(f, "\n-- Tracked entity instances (TEIs) per program\n")
    write(f, "SELECT 'Database summary - tracked entity instances per program' AS D2_DOCKER_PRESQL_SCRIPT;\n")
    write(f, """
SELECT
    p.uid  AS program_uid,
    p.name AS program_name,
    COUNT(DISTINCT e.trackedentityid) AS tei_count
FROM enrollment e
JOIN program p ON p.programid = e.programid
GROUP BY p.uid, p.name
ORDER BY p.name DESC;
""")


def _data_counts_query():
    """Rows (sort_order, owner, uid, name, item, total): data values per data set, events/enrollments/tracked
    entities per program and the whole database totals"""
    event_counts_per_stage = "\n            UNION ALL ".join(
        "SELECT programstageid, COUNT(*) AS total FROM {} GROUP BY programstageid".format(et.table)
        for et in get_event_tables())
    all_events = " + ".join("(SELECT COUNT(*) FROM {})".format(et.table) for et in get_event_tables())
    return """
        SELECT 0 AS sort_order, 'Whole database' AS owner, '' AS uid, '' AS name, 'data values' AS item,
               (SELECT COUNT(*) FROM datavalue) AS total
        UNION ALL SELECT 0, 'Whole database', '', '', 'events', {all_events}
        UNION ALL SELECT 0, 'Whole database', '', '', 'enrollments', (SELECT COUNT(*) FROM {enrollment})
        UNION ALL SELECT 0, 'Whole database', '', '', 'tracked entities', (SELECT COUNT(*) FROM {trackedentity})
        UNION ALL
        SELECT 1, 'Data set', ds.uid, ds.name, 'data values', COALESCE(SUM(dv.total), 0)
        FROM dataset ds
        LEFT JOIN datasetelement dse ON dse.datasetid = ds.datasetid
        LEFT JOIN (SELECT dataelementid, COUNT(*) AS total FROM datavalue GROUP BY dataelementid) dv
            ON dv.dataelementid = dse.dataelementid
        GROUP BY ds.uid, ds.name
        UNION ALL
        SELECT 2, 'Program', p.uid, p.name, 'events', COALESCE(SUM(ev.total), 0)
        FROM program p
        LEFT JOIN programstage ps ON ps.programid = p.programid
        LEFT JOIN (
            {event_counts_per_stage}
        ) ev ON ev.programstageid = ps.programstageid
        GROUP BY p.uid, p.name
        UNION ALL
        SELECT 3, 'Program', p.uid, p.name, 'enrollments', COUNT(en.{enrollmentid})
        FROM program p JOIN {enrollment} en ON en.programid = p.programid
        WHERE p.type = 'WITH_REGISTRATION'
        GROUP BY p.uid, p.name
        UNION ALL
        SELECT 4, 'Program', p.uid, p.name, 'tracked entities', COUNT(DISTINCT en.{trackedentityid})
        FROM program p JOIN {enrollment} en ON en.programid = p.programid
        WHERE p.type = 'WITH_REGISTRATION'
        GROUP BY p.uid, p.name
    """.format(all_events=all_events, event_counts_per_stage=event_counts_per_stage,
               enrollment=get_enrollment_table_name(), enrollmentid=get_enrollment_identifier_name(),
               trackedentity=get_tracker_table_name(), trackedentityid=get_tracker_identifier_name())


def snapshot_data_counts(f):
    """Store how much data there is before any deletion, so report_deleted_data_counts can show what was deleted.
    A temporary table, as the whole sql file runs in one psql session."""
    write(f, """
        SELECT 'Counting data values/events/enrollments/tracked entities before deletion....' AS D2_DOCKER_PRESQL_SCRIPT;
        DROP TABLE IF EXISTS {before};
        CREATE TEMP TABLE {before} AS {counts};
    """.format(before=COUNTS_BEFORE_TABLE, counts=_data_counts_query()))


def report_deleted_data_counts(f):
    """Human readable summary of what was deleted and what is left, per data set and program. A program that should
    have been emptied but still has data is the sign of a deletion that failed"""
    write(f, """
        SELECT 'Counting data values/events/enrollments/tracked entities after deletion....' AS D2_DOCKER_PRESQL_SCRIPT;
        DROP TABLE IF EXISTS {after};
        CREATE TEMP TABLE {after} AS {counts};
        SELECT 'Summary of deleted data (deleted / remaining):' AS D2_DOCKER_PRESQL_SCRIPT;
        SELECT
            CASE WHEN c.uid = '' THEN c.owner ELSE c.owner || ' ' || quote_literal(c.name) || ' (' || c.uid || ')' END
            || ': ' || (c.before_total - c.after_total) || ' ' || c.item || ' deleted, '
            || c.after_total || ' remaining' AS D2_DOCKER_PRESQL_SCRIPT
        FROM (
            SELECT COALESCE(b.sort_order, a.sort_order) AS sort_order, COALESCE(b.owner, a.owner) AS owner,
                   COALESCE(b.uid, a.uid) AS uid, COALESCE(b.name, a.name) AS name, COALESCE(b.item, a.item) AS item,
                   COALESCE(b.total, 0) AS before_total, COALESCE(a.total, 0) AS after_total
            FROM {before} b
            FULL JOIN {after} a ON a.owner = b.owner AND a.uid = b.uid AND a.item = b.item
        ) c
        WHERE c.before_total > 0 OR c.after_total > 0
        ORDER BY CASE WHEN c.sort_order = 1 THEN 1 WHEN c.sort_order = 0 THEN 0 ELSE 2 END, c.name, c.uid, c.sort_order, c.item;
        DROP TABLE IF EXISTS {before};
        DROP TABLE IF EXISTS {after};
    """.format(before=COUNTS_BEFORE_TABLE, after=COUNTS_AFTER_TABLE, counts=_data_counts_query()))
