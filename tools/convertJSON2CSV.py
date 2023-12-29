import json
import csv

json_path = '../config.json'

csv_path = 'depfromconfig.csv'


def process_dep(dep_name, programs, csv_writter):
    for programType, uids in programs.items():
        for uid in uids:
            csv_writter.writerow([dep_name, programType, uid])


with open(json_path, 'r') as archivo_json:
    data = json.load(archivo_json)


with open(csv_path, 'w', newline='') as csv_file:
    csv_writter = csv.writer(csv_file)

    csv_writter.writerow(['Dep_name', 'program_type', 'uid'])

    for departament, programs in data["departments"].items():
        process_dep(departament, programs, csv_writter)

print(f'Archivo CSV creado exitosamente: {csv_path}')