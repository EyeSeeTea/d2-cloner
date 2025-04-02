from src.preprocess.sql_generation.sql_common import convert_to_sql_format, write, fix_final_query


def generate_delete_datasets_rules(datasets, data_elements, org_units,
                                   org_unit_descendants, all_uid, f):
    write(f, "--remove datasets" + "\n")
    sql_all = convert_to_sql_format(all_uid)
    sql_datasets = convert_to_sql_format(datasets)
    sql_data_elements = convert_to_sql_format(data_elements)
    sql_org_units = convert_to_sql_format(org_units)
    sql_org_unit_descendants = convert_to_sql_format(org_unit_descendants)
    sql_query = """
        DELETE FROM datavalue where dataelementid in (select dataelementid from datasetelement 
        where datasetid in ( select datasetid from dataset where uid in {all})) and
    """.format(all=sql_all)
    has_rule = False
    if sql_data_elements != "":
        has_rule = True
        if sql_datasets != "":
            sql_query = sql_query + """ 
                dataelementid in (select dataelementid from dataelement where uid in {dataelements}  
                and dataelementid in (select dataelementid from datasetelement where datasetid  
                in ( select datasetid from dataset where uid in {datasets}))) and 
            """.format(dataelements=sql_data_elements, datasets=sql_datasets)
        else:
            sql_query = sql_query + """  
                 dataelementid in (select dataelementid from dataelement where uid in {dataelements} 
                 and dataelementid in (select dataelementid from datasetelement 
                 where datasetid in ( select datasetid from dataset where uid in {datasets}))) and 
            """.format(dataelements=sql_data_elements, datasets=sql_all)
    elif sql_datasets != "":
        has_rule = True
        sql_query = sql_query + """ 
             dataelementid in (select dataelementid from datasetelement
             where datasetid in ( select datasetid from dataset where uid in {datasets})) and 
        """.format(datasets=sql_datasets)
    if sql_org_units != "":
        has_rule = True
        sql_query = sql_query + """ 
            sourceid in (select organisationunitid from organisationunit where uid in {orgunits}) and 
        """.format(orgunits=sql_org_units)
    if sql_org_unit_descendants != "":
        has_rule = True
        sql_query = sql_query + """ 
            sourceid in (select organisationunitid from organisationunit where path 
            like (select concat(path,'/%') from organisationunit where uid in {oudescendants})) and 
        """.format(oudescendants=sql_org_unit_descendants)

    if not has_rule:
        delete_all_data_sets_from_lists(all_uid, f)
    else:
        write(f, fix_final_query(sql_query) + "\n")


def delete_all_data_sets_from_lists(programs, f):
    programs = convert_to_sql_format(programs)
    delete_all_data_sets(programs, f)


def delete_all_data_sets(datasets, f):
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
