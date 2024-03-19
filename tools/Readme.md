# Department List Editor

This Python script simplifies the management of department-related data in DHIS2 by facilitating the download and conversion of department data to and from CSV and JSON formats.

## Features

- **Download and Generate CSV**: Download all programs and datasets related to departments, including those already identified in a `.config` file. It generates a `.csv` file for easy identification and association of each item with its corresponding department.

- **Convert CSV to JSON**: Convert a department list from a `.csv` file to a `.json` format suitable for further processing or integration.

- **Convert JSON to CSV**: Convert a department list from `.json` format back to a `.csv` file for easy viewing and editing.

## Usage

### Download and Create CSV

To download data and create a `.csv` file, use the following command:

```
python departament_list_editor.py --user user --password $'password' --url $'http://localhost:8080' --departments-file config.json
```

This command downloads all programs and datasets for departments, creating a .csv file for easy management, adding the department to the already identified metadata in the config file. If --departments-file is not specified, it creates a list for all departments without know department.

Convert CSV to JSON
To convert a .csv file to .json format, use the following command with a csv file:

```
python departament_list_editor.py --convert departments_20240319.csv
```

Convert JSON to CSV
To convert a .json file back to .csv format, use the following command with a valid json file:

```
python departament_list_editor.py --convert config.json
```
