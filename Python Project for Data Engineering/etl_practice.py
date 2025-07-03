import glob
import pandas as pd
from datetime import datetime
import xml.etree.ElementTree as ET
# datasource.zip download by type this in terminal "wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/datasource.zip "
log_file = 'log_file.txt'
target_file = 'transformed_data.csv'

def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines=True)
    return dataframe

def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=['car_model','year_of_manufacture','price','fuel'])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for row in root:
        car_model = row.find('car_model').text
        year_of_manufacture = float(row.find('year_of_manufacture').text)
        price = float(row.find('price').text)
        fuel = row.find('fuel').text
        dataframe = pd.concat([dataframe,pd.DataFrame([{"car_model":car_model,'year_of_manufacture':year_of_manufacture,'price':price,'fuel':fuel}])], ignore_index = True)
    return dataframe
def extract():
    extracted_data = pd.DataFrame(columns=['car_model','year_of_manufacture','price','fuel'])

    for csvfile in glob.glob('*.csv'):
        if csvfile != target_file:
            extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index=True)

    for jsonfile in glob.glob('*.json'):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True)

    for xmlfile in glob.glob('*.xml'):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index=True)
    
    return extracted_data

def transform(data):
    data['price'] = round(data['price'], 2)
    return data

def load(target_file, transformed_data):
    transformed_data.to_csv(target_file)

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file,"a") as f:
        f.write(timestamp + ',' + message + '\n')

log_progress("ETL Job Started")


log_progress("starting extract")
extracted_data = extract()
log_progress("done extract")

log_progress("starting transform data")
transformed_data = transform(extracted_data)
log_progress("done transform data")

print("Here is transformed data")
print(transformed_data)

log_progress("Transform phase Ended") 
  

log_progress("Load phase Started") 
load(target_file,transformed_data) 
  

log_progress("Load phase Ended") 
  
log_progress("ETL Job Ended") 