import csv
import json

# Path to the input CSV file
csv_path = 'departamentsFromConfig.csv'

# Path to the output JSON file
json_path = 'departament_list.json'

# Initialize a dictionary to store the data
data = {'departaments': {}}

# Read the CSV file
with open(csv_path, mode='r', newline='') as csv_file:
    csv_reader = csv.DictReader(csv_file)

    # Iterate through the rows of the CSV
    for row in csv_reader:
        dept_name = row['Dep_name']
        program_type = row['program_type']
        uid = row['uid']

        # Ensure the department and program type exist in the dictionary
        if dept_name not in data['departaments']:
            data['departaments'][dept_name] = {}
        if program_type not in data['departaments'][dept_name]:
            data['departaments'][dept_name][program_type] = []

        # Add the uid to the corresponding program type
        data['departaments'][dept_name][program_type].append(uid)

# Write the data to a JSON file
with open(json_path, 'w') as json_file:
    json.dump(data, json_file, indent=4)

print(f'JSON file successfully created: {json_path}')
