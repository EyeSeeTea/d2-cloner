import requests
import csv
import base64

from requests.auth import HTTPBasicAuth

url_api = 'https://portal-uat.who.int/dhis2/api'

usuario = ''
contrasena = ''
credenciales = f"{usuario}:{contrasena}"
credenciales_codificadas = base64.b64encode(credenciales.encode()).decode()

csv_from_config="depfromconfig.csv"
# Initialize a dictionary to hold department names from the configuration file
dep_config = {}

# Read the department configuration CSV file
with open(csv_from_config, mode='r', newline='') as config_file:
    config_reader = csv.DictReader(config_file)
    for config_row in config_reader:
        # Map each uid to its corresponding department name from the config file
        dep_config[config_row['uid']] = config_row['Dep_name']


headers = {
    'Authorization': f'Basic {credenciales_codificadas}'
}
output_csv = 'departamentsfromdhis.csv'
auth=HTTPBasicAuth(usuario, contrasena)

response = requests.get(url_api + "/programs.json?fields=id,name,userGroupAccesses[displayName],programType&paging=false", headers=headers)


def getGroupNames(program):
    listOfGroups = program['userGroupAccesses']
    listOfGroupNames = ""
    for usergroup in listOfGroups:
        listOfGroupNames = listOfGroupNames + usergroup.get("displayName") + ","
    listOfGroupNames = listOfGroupNames[:-1]
    return listOfGroupNames


if response.status_code == 200:
    programs = response.json()

    dataSetsResponse = requests.get(url_api + "/dataSets.json?fields=id,name,userGroupAccesses[displayName]&paging=false", headers=headers)

    dataSets = dataSetsResponse.json()
    # Crea y escribe en el archivo CSV
    with open(output_csv, 'w', newline='') as archivo_csv:
        # Define los encabezados del CSV

        campos = ['uid', 'Dep_name', 'program_name', 'program_type', 'groups', 'action']
        write_csv = csv.DictWriter(archivo_csv, fieldnames=campos)


        write_csv.writeheader()
        for program in programs["programs"]:
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

        for dataSet in dataSets["dataSets"]:
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
    print(f'Archivo CSV creado exitosamente: {output_csv}')
else:
    print(f'Error al realizar la llamada a la API: {response.status_code}')