#etl code

import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

class ETL:
    def __init__(self, log_file="log_file.txt", target_path="output_data") -> None:
        self.log_file = log_file
        self.target_path = target_path
    
    def extract_from_csv(self, file_path: str) -> pd.DataFrame:
        df = pd.read_csv(file_path)
        return df
    
    def extract_from_json(self, file_path: str) -> pd.DataFrame:
        df = pd.read_json(file_path, lines=True)
        return df
    
    def extract_from_xml(self, file_path: str) -> pd.DataFrame:
        df = pd.DataFrame(columns=[ 'car_model', 'year_of_manufacture', 'price', 'fuel'])
        tree = ET.parse(file_path)
        root = tree.getroot()
        for car in root:
            car_model = car.find("car_model").text
            year_of_manufacture = car.find("year_of_manufacture").text
            price = float(car.find("price").text)
            fuel = car.find("fuel").text
            df = pd.concat([df, pd.DataFrame([{"car_model":car_model, "year_of_manufacture":year_of_manufacture, "price":price, "fuel":fuel}])], ignore_index=True)
        return df

    def extract(self) -> pd.DataFrame:
        extracted_data = pd.DataFrame(columns=[ 'car_model', 'year_of_manufacture', 'price', 'fuel'])

        for csv_file in glob.glob("data/*.csv"):
            extracted_data = pd.concat([extracted_data, pd.DataFrame(self.extract_from_csv(csv_file))], ignore_index=True)
        
        for json_file in glob.glob("data/*.json"):
            extracted_data = pd.concat([extracted_data, pd.DataFrame(self.extract_from_json(json_file))], ignore_index=True)

        for xml_file in glob.glob("data/*.xml"):
            extracted_data = pd.concat([extracted_data, pd.DataFrame(self.extract_from_xml(xml_file))], ignore_index=True)
        
        return extracted_data
    
    def transform(data: pd.DataFrame) -> pd.DataFrame:

        data['price'] = round(data.price,2) 
        
        return data 
    
    def load_data(self, target_path, transformed_data):
        transformed_data.to_csv(target_path)

    def log_progress(self, message: str):
        timestamp_format = '%Y-%h-%d-%H:%M:%S'
        now = datetime.now()
        timestamp = now.strftime(timestamp_format)
        with open(self.log_file, "a") as f:
            f.write(timestamp + ',' + message + '/n')

etl = ETL(target_path="output_file.csv")

etl.log_progress("ETL job started")

etl.log_progress("Extract phase Started") 
extracted_data = etl.extract() 

etl.log_progress("Extract phase Ended") 
transformed_data = ETL.transform(extracted_data)

print("Transformed Data") 
print(transformed_data) 
 
# Log the completion of the Transformation process 
etl.log_progress("Transform phase Ended") 
 
# Log the beginning of the Loading process 
etl.log_progress("Load phase Started") 

etl.load_data(etl.target_path, transformed_data, )

# Log the completion of the Loading process 
etl.log_progress("Load phase Ended") 

# Log the completion of the ETL process 
etl.log_progress("ETL Job Ended")