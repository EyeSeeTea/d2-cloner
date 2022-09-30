import os

from src.preprocess.query_generator import generate_delete_datasets_rules, generate_delete_tracker_rules, \
    generate_delete_event_rules, delete_all_event_programs, delete_all_data_sets, delete_all_tracker_programs, \
    remove_all_unnecessary_dependencies, generate_anonymize_tracker_rules, generate_anonymize_event_rules, \
    generate_anonymize_datasets_rules, generate_anonymize_user_queries

import shutil

anonymize_rule = "anonymizeData"
remove_rule = "removeData"
program_type = "eventPrograms"
tracker_type = "trackerPrograms"
dataset_type = "dataSets"
users_type = "users"
select_new_admin_user = "selectAdminUser"
select_old_admin_user = "selectOldAdminUser"
exclude_user_names = "excludeUsernames"
anonymize_users = "anonymizeUsers"
metadata_type = "selectMetadataType"
file_name = "preprocess.sql"
actions = "actions"
select_datasets = "selectDatasets"
select_event_program = "selectEventProgram"
select_tracker_program = "selectTrackerProgram"
select_org_units = "selectOrgUnits"
select_org_unit_and_descendants = "selectOrgUnitAndDescendants"
anonymize_phone_rule = "anonymizePhone"
anonymize_mail_rule = "anonymizeMail"
anonymize_org_unit_rule = "anonymizeOrgUnit"
anonymize_coordinates_rule = "anonymizeCoordinates"
select_data_elements = "selectDataElements"
select_tracked_entity_attributes = "selectTrackedEntityAttributes"
action = "action"
select_departament = "selectDepartament"
default_api_version = "36"

def get_file():
    return file_name


def move_file(file, new_folder):
    shutil.move(file, new_folder)


def create_dir_if_not_exists(directory):
    try:
        os.makedirs(directory)
    except FileExistsError:
        # directory already exists
        pass


def preprocess(entries, departments, directory, preprocess_api_version):
    if preprocess_api_version == None:
        preprocess_api_version = default_api_version
    create_dir_if_not_exists(directory)
    f = open(os.path.join(directory, get_file()), "w")
    f.write("-- Autogenerated preprocess sql file" + "\n")
    add_rules_by_departament(departments, entries)
    remove_all_unnecessary_dependencies(f, preprocess_api_version)
    generate_queries(departments, f, preprocess_api_version)
    f.close()


def remove_all(list_uid, f):
    for key in list_uid.keys():
        if key == program_type:
            delete_all_event_programs(list_uid[key], f)
        if key == dataset_type:
            delete_all_data_sets(list_uid[key], f)
        if key == tracker_type:
            delete_all_tracker_programs(list_uid[key], f)


def generate_queries(departament, f, preprocess_api_version):
    for key in departament.keys():
        f.write("--Departament:" + key + "\n")
        print("--Departament:" + key + "\n")
        if actions not in departament[key]:
            continue
        for rule in departament[key][actions]:
            has_datasets = False
            has_event_program = False
            has_tracker_program = False
            if rule[action] == anonymize_users:
                new_admin = get_rule_content(rule, select_new_admin_user)
                old_admin = get_rule_content(rule, select_old_admin_user)
                exclude_users = get_rule_content(rule, exclude_user_names)
                generate_anonymize_user_queries(new_admin, old_admin, exclude_users, f, preprocess_api_version)

            elif rule[action] == remove_rule or rule[action] == anonymize_rule:

                # get metadata types
                if metadata_type in rule.keys():
                    has_datasets = check_if_has_metadata_type(rule, dataset_type)
                    has_event_program = check_if_has_metadata_type(rule, program_type)
                    has_tracker_program = check_if_has_metadata_type(rule, tracker_type)

                # gets uid list if exists
                datasets = get_rule_content(rule, select_datasets)
                event_program = get_rule_content(rule, select_event_program)
                tracker_program = get_rule_content(rule, select_tracker_program)

                # get restriction if exist
                org_units = get_rule_content(rule, select_org_units)
                org_unit_descendants = get_rule_content(rule, select_org_unit_and_descendants)
                anonimize_org_units = get_rule_content(rule, anonymize_org_unit_rule)
                anonimize_mail = get_rule_content(rule, anonymize_mail_rule)
                anonimize_phone = get_rule_content(rule, anonymize_phone_rule)
                anonimize_coordinate = get_rule_content(rule, anonymize_coordinates_rule)
                data_elements = get_rule_content(rule, select_data_elements)
                tracker_entity_attributes = get_rule_content(rule, select_tracked_entity_attributes)

                if rule[action] == remove_rule:
                    if has_datasets:
                        generate_delete_datasets_rules(datasets, data_elements, org_units,
                                                       org_unit_descendants, departament[key][dataset_type], f,
                                                       preprocess_api_version)
                    if has_tracker_program:
                        generate_delete_tracker_rules(tracker_program, data_elements, org_units,
                                                      org_unit_descendants, departament[key][tracker_type], f,
                                                      preprocess_api_version)
                    if has_event_program:
                        generate_delete_event_rules(event_program, data_elements, org_units,
                                                    org_unit_descendants, departament[key][program_type], f,
                                                    preprocess_api_version)
                    # if not have specific rules apply to all the departament
                    if not has_datasets and not has_event_program and not has_tracker_program:
                        remove_all(departament[key], f)
                elif rule[action] == anonymize_rule:
                    if has_datasets:
                        generate_anonymize_datasets_rules(datasets, org_units, data_elements,
                                anonimize_org_units, anonimize_phone, anonimize_mail,
                                anonimize_coordinate, departament[key][dataset_type], f, preprocess_api_version)
                    if has_tracker_program:
                        generate_anonymize_tracker_rules(tracker_program, tracker_entity_attributes, org_units, data_elements,
                                anonimize_org_units, anonimize_phone, anonimize_mail,
                                anonimize_coordinate,
                                                         departament[key][tracker_type], f, preprocess_api_version)
                    if has_event_program:
                        generate_anonymize_event_rules(event_program, org_units, data_elements,
                                anonimize_org_units, anonimize_phone, anonimize_mail,
                                anonimize_coordinate,
                                                       departament[key][program_type], f, preprocess_api_version)
                    # if not have specific rules apply to all the departament
                    if not has_datasets and not has_event_program and not has_tracker_program:
                        for key_type in departament[key].keys():
                            if key_type == program_type:
                                generate_anonymize_event_rules(event_program, org_units, data_elements,
                                                               anonimize_org_units, anonimize_phone, anonimize_mail,
                                                               anonimize_coordinate,
                                                               departament[key][program_type], f, preprocess_api_version)
                            elif key_type == dataset_type:
                                generate_anonymize_datasets_rules(datasets, org_units, data_elements,
                                anonimize_org_units, anonimize_phone, anonimize_mail,
                                anonimize_coordinate, departament[key][dataset_type], f, preprocess_api_version)
                            elif key_type == tracker_type:
                                generate_anonymize_tracker_rules(tracker_program, tracker_entity_attributes,
                                                                 org_units, data_elements,
                                                                 anonimize_org_units, anonimize_phone, anonimize_mail,
                                                                 anonimize_coordinate,
                                                                 departament[key][tracker_type], f, preprocess_api_version)


def get_rule_content(rule, rule_type):
    if rule_type in rule.keys():
        return rule[rule_type]
    elif rule_type in [anonymize_phone_rule, anonymize_coordinates_rule, anonymize_mail_rule]:
        return True
    elif rule_type == anonymize_org_unit_rule:
        return False
    else:
        return ""


def check_if_has_metadata_type(rule, check_type):
    exist = False
    for rule_type in rule[metadata_type]:
        if rule_type == check_type:
            exist = True
    return exist


def add_rules_by_departament(departament, entries):
    for entry in entries:
        exist = False
        for key in departament.keys():
            if entry[select_departament].upper() == key.upper():
                if actions not in departament[key].keys():
                    departament[key][actions] = list()
                departament[key][actions].append(entry)
                exist = True
        if not exist:
            print("--unknown departament key for entry:" + entry[select_departament])
