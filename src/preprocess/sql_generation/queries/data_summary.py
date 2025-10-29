from src.preprocess.sql_generation.sql_common import write


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
FROM event e
JOIN programstage ps ON e.programstageid = ps.programstageid
JOIN program p ON ps.programid = p.programid
GROUP BY p.uid, p.name
ORDER BY p.name DESC;
""")

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