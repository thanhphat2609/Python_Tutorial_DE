import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime


log_file = "./transformed/log_file.txt"

target_file = "./transformed/transformed_data.csv"


# Extract data from csv files
def extract_from_csv(csv_files):
    dataframe = pd.read_csv(csv_files)
    return dataframe

# Extract data from json files
def extract_from_json(json_files):
    dataframe = pd.read_json(json_files, lines=True)
    return dataframe

# Extract data from xml files
    # To extract from an XML file, you need first to parse the data from the file using the ElementTree function. 
    # You can then extract relevant information from this data and append it to a pandas dataframe as follows.
def extract_from_xml(xml_files): 
    dataframe = pd.DataFrame(columns=["name", "height", "weight"]) 
    tree = ET.parse(xml_files) 
    root = tree.getroot() 
    for person in root: 
        name = person.find("name").text 
        height = float(person.find("height").text) 
        weight = float(person.find("weight").text) 
        dataframe = pd.concat([dataframe, pd.DataFrame([{"name": name, "height": height, "weight": weight}])], ignore_index=True) 
    return dataframe 


# Create a Extract function for reading all dataframe into 1 dataframe
def extract():

    extracted_data = pd.DataFrame(columns=['name','height','weight']) # create an empty data frame to hold extracted data 

    # Process all csv files
    for csvfile in glob.glob("./dataset/*.csv"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index = True)

    # Process all json files
    for jsonfile in glob.glob("./dataset/*.json"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index = True)

    # Process all xml files
    for xmlfile in glob.glob("./dataset/*.xml"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index = True) 
    
    return extracted_data

# Transform data after extract
def transform(data):
    '''Convert inches to meters and round off to two decimals 
    1 inch is 0.0254 meters '''
    data['height'] = round(data.height * 0.0254,2) 
 
    '''Convert pounds to kilograms and round off to two decimals 
    1 pound is 0.45359237 kilograms '''
    data['weight'] = round(data.weight * 0.45359237,2)

    return data

# Load data
def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)

# Log result of progress
def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current time
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ',' + message + '\n')

# Log the initialization of the ETL process 
log_progress("ETL Job Started") 
 
# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
extracted_data = extract() 
 
# Log the completion of the Extraction process 
log_progress("Extract phase Ended") 
 
# Log the beginning of the Transformation process 
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data) 
print("Transformed Data") 
print(transformed_data) 
 
# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 
 
# Log the beginning of the Loading process 
log_progress("Load phase Started") 
load_data(target_file,transformed_data) 
 
# Log the completion of the Loading process 
log_progress("Load phase Ended") 
 
# Log the completion of the ETL process 
log_progress("ETL Job Ended") 

