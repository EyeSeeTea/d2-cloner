from datetime import datetime
import requests
import csv
import base64

from requests.auth import HTTPBasicAuth

import argparse
import json
import csv


def convertCSV2JSON(csv_path, json_path):
    # Initialize a dictionary to store the data
    data = {'departments': {}}

    # Read the CSV file
    with open(csv_path, mode='r', newline='') as csv_file:
        csv_reader = csv.DictReader(csv_file)

        # Iterate through the rows of the CSV
        for row in csv_reader:
            dept_name = row['Dep_name']
            program_type = row['program_type']
            uid = row['uid']

            # Ensure the department and program type exist in the dictionary
            if dept_name not in data['departments']:
                data['departments'][dept_name] = {}
            if program_type not in data['departments'][dept_name]:
                data['departments'][dept_name][program_type] = []

            # Add the uid to the corresponding program type
            data['departments'][dept_name][program_type].append(uid)

    # Write the data to a JSON file
    with open(json_path, 'w') as json_file:
        json.dump(data, json_file, indent=4)

    print(f'JSON file successfully created: {json_path}')


def convertJSON2CSV(json_path, csv_path):
    with open(json_path, 'r') as archivo_json:
        data = json.load(archivo_json)

    with open(csv_path, 'w', newline='') as csv_file:
        csv_writter = csv.writer(csv_file)

        csv_writter.writerow(['Dep_name', 'program_type', 'uid'])

        for departments, programs in data["departments"].items():
            process_dep(departments, programs, csv_writter)
    print(f'CSV file successfully created: {csv_path}')


def process_dep(dep_name, programs, csv_writter):
    for programType, uids in programs.items():
        for uid in uids:
            csv_writter.writerow([dep_name, programType, uid])

    
def getGroupNames(program):
    listOfGroups = program['userGroupAccesses']
    listOfGroupNames = ""
    for usergroup in listOfGroups:
        listOfGroupNames = listOfGroupNames + usergroup.get("displayName") + ","
    listOfGroupNames = listOfGroupNames[:-1]
    return listOfGroupNames

def validate_download_args(args):
    if not args.url or not args.user or not args.password:
        raise ValueError('Server URL, user, and password are required for download mode')
    
def pull_programs(server_url, headers):
    response = requests.get(server_url + "/api/programs.json?fields=id,name,userGroupAccesses[displayName],programType&paging=false", headers=headers)
    if response.status_code == 200:
        programs = response.json()
        return programs
    else:
        raise ValueError(f'Error pulling programs from DHIS2: {response.status_code} - {response.text}')
    
def pull_datasets(server_url, headers):
    response = requests.get(server_url + "/api/dataSets.json?fields=id,name,userGroupAccesses[displayName]&paging=false", headers=headers)
    if response.status_code == 200:
        dataSets = response.json()
        return dataSets
    else:
        raise ValueError(f'Error pulling datasets from DHIS2: {response.status_code} - {response.text}')

def pull_metadata(server_url, headers):
    datasets = pull_datasets(server_url, headers)
    programs = pull_programs(server_url, headers)
    return {"dataSets": datasets["dataSets"], "programs": programs["programs"]}
    
def save_departments_to_csv(metadata, output_filename, file=None):
    dataSets = metadata[0]["dataSets"]
    programs = metadata[0]["programs"]
    dep_config = {}
    #read already identified departments
    if file and file.endswith('.json'):
        with open(file, 'r') as archivo_json:
            data = json.load(archivo_json)
            json_departments = data["departments"]
            for department_key in json_departments.keys():
                    item = json_departments[department_key]
                    if "eventPrograms" in item.keys():
                        for uid in item["eventPrograms"]:
                            dep_config[uid] = department_key
                    if "trackerPrograms" in item.keys():
                        for uid in item["trackerPrograms"]:
                            dep_config[uid] = department_key
                    if "dataSets" in item.keys():
                        for uid in item["dataSets"]:
                            dep_config[uid] = department_key
    print(dep_config)
    #write to csv:
    with open(output_filename+".csv", 'w', newline='') as archivo_csv:

        csv_fields = ['uid', 'Dep_name', 'program_name', 'program_type', 'groups', 'action']
        write_csv = csv.DictWriter(archivo_csv, fieldnames=csv_fields)
        write_csv.writeheader()

        for program in programs:
            dep_name = ""
            if program.get('id') in dep_config:
                dep_name = dep_config[program.get('id')]
            programType= ""
            if program.get('programType') == "WITHOUT_REGISTRATION":
                programType= "eventProgram"
            else:
                programType= "trackerProgram"
            listOfGroupNames=getGroupNames(program)
            write_csv.writerow({
                'uid': program.get('id'),
                'Dep_name': dep_name,
                'program_name': program.get('name'),
                'program_type': programType,
                'groups': listOfGroupNames,
                'action': ""
            })

        for dataSet in dataSets:
            dep_name = ""
            if dataSet.get('id') in dep_config:
                dep_name = dep_config[dataSet.get('id')]
            programType= "dataSets"
            listOfGroupNames=getGroupNames(dataSet)
            write_csv.writerow({
                'uid': dataSet.get('id'),
                'Dep_name': dep_name,
                'program_name': dataSet.get('name'),
                'program_type': programType,
                'groups': listOfGroupNames,
                'action': ""
            })
    print(f'CSV file successfully created: {output_filename+".csv"}')


def main():
    parser = argparse.ArgumentParser(description='This tool make simple convert csv with the departments to the config format or update the config departments from dhis2.')
    parser.add_argument('--user', required=False, help='User name')
    parser.add_argument('--password', required=False, help='Password')
    parser.add_argument('--url', required=False, help='URL to process')
    parser.add_argument('--departments-file', required=False, help="""Json files: Config file or a file with the config file format for the departments: {"departments": ["dep_name":["uid","uid"]]}. 
                        Csv file: File with the departments and the programs to convert to valid json format. """)
    parser.add_argument('--convert', required=False, help='Convert file. If the file is CSV, it will be converted to JSON and vice versa.')

    args = parser.parse_args()

    output_filename = "departments_"+datetime.now().strftime("%Y%m%d")
    file = args.departments_file

    if args.convert:
        if args.convert.endswith('.csv'):
            convertCSV2JSON(args.convert, output_filename+".json")
        else:
            convertJSON2CSV(args.convert, output_filename+".csv")
    else:
        validate_download_args(args)   
        creedentials = f"{args.user}:{args.password}"
        creedentials = base64.b64encode(creedentials.encode()).decode()
        headers = {
            'Authorization': f'Basic {creedentials}'
        }
        server_url = args.url
        metadata = pull_metadata(server_url, headers), 
        save_departments_to_csv(metadata, output_filename, file)



if __name__ == '__main__':
    main()