from src.preprocess.sql_generation.sql_common import convert_to_sql_format, write, fix_final_query


def generate_delete_datasets_rules(datasets, data_elements, org_units,
                                   org_unit_descendants, all_uid, f):
    sql_all = convert_to_sql_format(all_uid)
    sql_datasets = convert_to_sql_format(datasets)
    dataset_uids_sql = sql_datasets if sql_datasets != "" else sql_all
    write(f, f"""
    SELECT 'Starting DELETE block  for dataSets: All: ' || quote_literal($${sql_all or 'NO_IDS'}$$) ||
           ' and Detailed: ' || quote_literal($${sql_datasets or 'NO_IDS'}$$) AS D2_DOCKER_PRESQL_SCRIPT;
    """)
    sql_data_elements = convert_to_sql_format(data_elements)
    sql_org_units = convert_to_sql_format(org_units)
    sql_org_unit_descendants = convert_to_sql_format(org_unit_descendants)

    if sql_data_elements != "" or sql_org_units != "" or sql_org_unit_descendants != "":
        sql_query = compose_custom_query(dataset_uids_sql, sql_data_elements, sql_org_units, sql_org_unit_descendants)
        write(f, fix_final_query(sql_query) + "\n")
    else:
        delete_all_data_sets(dataset_uids_sql, f, False)

    write(f, f"""
    SELECT 'Close DELETE Dataset Block' AS D2_DOCKER_PRESQL_SCRIPT;
    """)


def compose_custom_query(datasets_uids_program_uids, sql_data_elements, sql_org_units, sql_org_unit_descendants):
    sql_query = """
            DELETE FROM datavalue where dataelementid in (select dataelementid from datasetelement 
            where datasetid in ( select datasetid from dataset where uid in {all})) and 
        """.format(all=datasets_uids_program_uids)
    if sql_data_elements != "":
            sql_query = sql_query + """  
                     dataelementid in (select dataelementid from dataelement where uid in {dataelements} 
                     and dataelementid in (select dataelementid from datasetelement 
                     where datasetid in ( select datasetid from dataset where uid in {datasets}))) and 
                """.format(dataelements=sql_data_elements, datasets=datasets_uids_program_uids)
    if sql_org_units != "":
        sql_query = sql_query + """ 
                sourceid in (select organisationunitid from organisationunit where uid in {orgunits}) and 
            """.format(orgunits=sql_org_units)
    if sql_org_unit_descendants != "":
        sql_query = sql_query + """ 
                sourceid in (SELECT DISTINCT child.organisationunitid
                FROM organisationunit AS child
                JOIN organisationunit AS parent
                ON child.path LIKE parent.path || '/%'
                WHERE parent.uid IN {oudescendants}) and 
            """.format(oudescendants=sql_org_unit_descendants)
    return sql_query


def delete_all_data_sets_from_lists(datasets, f):
    datasets_uids_sql = convert_to_sql_format(datasets)
    delete_all_data_sets(datasets_uids_sql, f, False)


def delete_all_data_sets_not_in_lists(datasets, f):
    datasets_uids_sql = convert_to_sql_format(datasets)
    delete_all_data_sets(datasets_uids_sql, f, True)


def delete_all_data_sets(datasets, f, exclude=False):
    write(f, f"""
    SELECT 'Starting DELETE block  for dataSets: All: ' || quote_literal($${datasets or 'NO_IDS'}$$) AS D2_DOCKER_PRESQL_SCRIPT;
    """)
    operator = "not in" if exclude else "in"
    #Show number of values to be removed
    write(f, f"""
      SELECT 'DataValues to be deleted: ' || COUNT(*)::text AS "D2_DOCKER_PRESQL_SCRIPT"
      FROM datavalue
      WHERE dataelementid {operator} (
        SELECT de.dataelementid
        FROM datasetelement de
        WHERE de.datasetid IN (
          SELECT d.datasetid FROM dataset d WHERE d.uid IN {datasets}
        )
      );

      SELECT 'DataValueAudits to be deleted: ' || COUNT(*)::text AS "D2_DOCKER_PRESQL_SCRIPT"
      FROM datavalueaudit
      WHERE dataelementid {operator} (
        SELECT de.dataelementid
        FROM datasetelement de
        WHERE de.datasetid IN (
          SELECT d.datasetid FROM dataset d WHERE d.uid IN {datasets}
        )
      );
    """)

    write(f, "SELECT 'Deleting DataValueAudit....' AS D2_DOCKER_PRESQL_SCRIPT;\n")
    write(f, """
        DELETE FROM datavalueaudit where dataelementid {operator}
        (select dataelementid from datasetelement
        where datasetid in (select datasetid from dataset where uid in {datasets}));
    """.format(datasets=datasets, operator=operator))

    write(f, "SELECT 'Deleting DataValue....' AS D2_DOCKER_PRESQL_SCRIPT;\n")
    write(f, """
        DELETE FROM datavalue where dataelementid {operator} (select dataelementid from datasetelement
        where datasetid in (select datasetid from dataset where uid in {datasets}));
    """.format(datasets=datasets, operator=operator))
