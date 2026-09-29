import re


def write(f, text):
    f.write(text)


def write_or(f):
    write(f, " or \n")


def write_end_of_sentence(f):
    write(f, " );\n")


def convert_to_sql_format(list_uid):
    if len(list_uid) == 0:
        return ""
    return "(" + ", ".join(["'{}'".format(uid) for uid in list_uid]) + ")"


def convert_to_possible_paths_in_sql_format(list_uid):
    if len(list_uid) == 0:
        return ""
    return "and ( path like " + " or path like  " \
                                "".join(["'%{}%'".format(uid) for uid in list_uid]) + \
        ")".replace("(or", " ")

def analyze_before_delete(f):
    """Refresh statistics on the whole database before any DELETE below. A freshly
    restored/imported database has no statistics until autovacuum catches up, which
    can make the planner pick a bad plan (e.g. a seq scan) for the deletion queries."""
    write(f, """
        SELECT 'Refreshing statistics before deletion....' AS D2_DOCKER_PRESQL_SCRIPT;
        ANALYZE;
    """)


def drop_temp_indexes(f):
    """Drop every index created as temp_idx_* to speed up the deletion above, once it's no longer needed"""
    write(f, """
        SELECT 'Dropping temporary deletion indexes....' AS D2_DOCKER_PRESQL_SCRIPT;
        DO $$
        DECLARE
            idx RECORD;
        BEGIN
            FOR idx IN SELECT indexname FROM pg_indexes WHERE indexname LIKE 'temp\\_idx\\_%' LOOP
                EXECUTE 'DROP INDEX IF EXISTS ' || quote_ident(idx.indexname);
            END LOOP;
        END $$;
    """)


def vacuum_full_analyze(f):
    """Reclaim disk space and refresh statistics after the bulk deletes above.
    VACUUM FULL rewrites tables one at a time, so the extra disk space it needs
    temporarily is bounded by the single largest table being compacted, not the
    whole database at once."""
    write(f, """
        SELECT 'Running VACUUM FULL ANALYZE to reclaim space and refresh stats....' AS D2_DOCKER_PRESQL_SCRIPT;
        VACUUM FULL ANALYZE;
    """)


def fix_final_query(sql_query):
    """This method is required now to create the sql with the ; and remove possible unnecessary and;"""
    sql_query = sql_query + ";"
    sql_query = remove_spaces_before_semicolon(sql_query)
    sql_query = sql_query.replace("and;", ";")
    return sql_query

def remove_spaces_before_semicolon(s):
    return re.sub(r'(\S)\s+;', r'\1;', s)