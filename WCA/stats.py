import requests      
import pandas as pd      
from pyspark.sql import SparkSession  
    
# Define a list of cuber IDs      
cuber_list = ['2012PARK03', '2015BORR01', '2014WANG48', '2009ZEMD01', '2016KOLA02', '2008BROW01', '2023KRAF01']      
    
# Create an empty list to store the extracted data      
extract_list = []      
    
# Loop through the list of cuber IDs      
for cuber in cuber_list:      
    # Define the API endpoint URL for the current cuber      
    endpoint = f"https://www.worldcubeassociation.org/api/v0/persons/{cuber}"      
    
    # Send a GET request to the API endpoint with HTTP basic authentication and a timeout of 3 seconds      
    response = requests.get(endpoint, auth=auth_values, timeout=3)      
    
    # Check if the response status code is 200 (OK)      
    if response.status_code == 200:      
        # If the response was successful, extract the data from the response and store it in a dictionary      
        extract_dict = response.json()     
    
        # Flatten the nested JSON data using json_normalize()      
        flattened_data = pd.json_normalize(extract_dict)      
            
        # Select only the columns you want to keep    
        flattened_data = flattened_data[['competition_count', 'person.name', 'person.wca_id', 'person.gender', 'person.country.id', 'person.country.continentId', 'personal_records.222.single.best', 'personal_records.333.single.best', 'personal_records.333.average.best', 'personal_records.333.single.world_rank', 'personal_records.333oh.single.best', 'personal_records.333oh.average.best']]    
            
        # Rename the columns    
        flattened_data = flattened_data.rename(columns={    
            'competition_count': 'Competition Count',    
            'person.name': 'Name',    
            'person.wca_id': 'WCA ID',    
            'person.gender': 'Gender',    
            'person.country.id': 'Country ID',    
            'person.country.continentId': 'Continent ID',    
            'personal_records.222.single.best': '2x2 Single',    
            'personal_records.333.single.best': '3x3 Single',    
            'personal_records.333.average.best': '3x3 Average',    
            'personal_records.333.single.world_rank': '3x3 World Rank',    
            'personal_records.333oh.single.best': '3x3 OH Single',    
            'personal_records.333oh.average.best': '3x3 OH Average'    
        })    
            
        # Add the extracted data to the list      
        extract_list.append(flattened_data)      
      
    else:      
        # If the response was not successful, print the error message to the console      
        print(response.raise_for_status())      
    
# Concatenate the list of DataFrames into a single DataFrame      
df = pd.concat(extract_list)      
  
# Create a SparkSession object      
#spark = SparkSession.builder.appName("MyApp").getOrCreate()  
  
# Create a Spark DataFrame from the Pandas DataFrame      
spark_df = spark.createDataFrame(df)  
  
display(spark_df)
