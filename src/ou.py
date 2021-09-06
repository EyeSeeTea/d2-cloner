import json
import csv

with open('organisationUnitspaho.csv', mode='w') as employee_file:
    employee_writer = csv.writer(employee_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

    employee_writer.writerow(['Id', 'name', 'shortName', 'level', 'path'])

    with open('organisationUnitspaho.json') as json_file:
        data = json.load(json_file)
        for p in data['organisationUnits']:
            employee_writer.writerow([p['id'], p['name'], p['shortName'],p['level'],p['path']])
