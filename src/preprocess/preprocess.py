import os

from src.preprocess.query_generator import generate_delete_datasets_rules, generate_delete_tracker_rules, \
    generate_delete_event_rules, delete_all_event_programs, delete_all_data_sets, delete_all_tracker_programs, \
    remove_all_unnecessary_dependencies, generate_delete_org_unit_tree_rules
import shutil

remove_orgunit_tree = "removeOrganisationUnitTree"
remove_rule = "removeData"
program_type = "eventPrograms"
tracker_type = "trackerPrograms"
dataset_type = "dataSets"
metadata_type = "selectMetadataType"
file_name = "preprocess.sql"

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


def preprocess(entries, departments, directory):
    create_dir_if_not_exists(directory)
    f = open(os.path.join(directory, get_file()), "w")
    f.write("-- Autogenerated preprocess sql file"+ "\n")
    add_rules_by_departament(departments, entries)
    remove_all_unnecessary_dependencies(f)
    generate_queries(departments, f)
    f.close()


def remove_all(list_uid, f):
    for key in list_uid.keys():
        if key == program_type:
            delete_all_event_programs(list_uid[key], f)
        if key == dataset_type:
            delete_all_data_sets(list_uid[key], f)
        if key == tracker_type:
            delete_all_tracker_programs(list_uid[key], f)


def generate_queries(departament, f):
    for key in departament.keys():
        f.write("--"+key + "\n")
        if "actions" not in departament[key]:
            continue
        for rule in departament[key]["actions"]:
            has_datasets = False
            has_event_program = False
            has_tracker_program = False
            if rule["action"] == remove_orgunit_tree:
                generate_delete_org_unit_tree_rules(rule["selectOrganisationUnit"], f)

            if rule["action"] == remove_rule:
                datasets = ""
                event_program = ""
                tracker_program = ""
                if metadata_type in rule.keys():
                    for rule_type in rule[metadata_type]:
                        if rule_type.lower() == dataset_type:
                            has_datasets = True
                        if rule_type.lower() == program_type:
                            has_event_program = True
                        if rule_type.lower() == tracker_type:
                            has_tracker_program = True
                        if "selectDatasets" in rule.keys():
                            datasets = rule["selectDatasets"]
                            if dataset_type not in rule_type:
                                has_datasets = True
                        if "selectEventProgram" in rule.keys():
                            event_program = rule["selectEventProgram"]
                            if program_type not in rule_type:
                                has_event_program = True
                        if "selectTrackerProgram" in rule.keys():
                            tracker_program = rule["selectTrackerProgram"]
                            if tracker_program not in rule_type:
                                has_tracker_program = True

                org_units = ""
                if "selectOrgUnits" in rule.keys():
                    org_units = rule["selectOrgUnits"]
                org_unit_descendants = ""
                if "selectOrgUnitAndDescendants" in rule.keys():
                    org_unit_descendants = rule["selectOrgUnitAndDescendants"]
                data_elements = ""
                if "selectDataElements" in rule.keys():
                    data_elements = rule["selectDataElements"]

                if has_datasets:
                    generate_delete_datasets_rules(datasets, data_elements, org_units, org_unit_descendants, departament[key][dataset_type], f)
                if has_tracker_program:
                    generate_delete_tracker_rules(tracker_program, data_elements, org_units, org_unit_descendants, departament[key][tracker_type], f)
                if has_event_program:
                    generate_delete_event_rules(event_program, data_elements, org_units, org_unit_descendants, departament[key][program_type], f)
                if not has_datasets and not has_event_program and not has_tracker_program:
                    remove_all(departament[key], f)


def add_rules_by_departament(departament, entries):
    for entry in entries:
        exist = False
        for key in departament.keys():
            if entry["selectDepartament"].upper() == key.upper():
                if "actions" not in departament[key].keys():
                    departament[key]["actions"] = list()
                departament[key]["actions"].append(entry)
                exist = True
        if not exist:
            print("--unknown departament key for entry:" + entry["selectDepartament"])
