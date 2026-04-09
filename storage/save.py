import datetime
import json

def save_raw(data, file_name="raw_data.json"):
    """
    Save the raw data to a JSON file.
    
    Parameters:
    - data: The raw data to be saved (Dataframe).
    - file_name: The name of the JSON file to save the data to (default is "raw_data.json").
    
    This function uses the json module to write the raw data to a file in JSON format, which is a common format for storing and exchanging data.
    """
    # Add a timestamp to the file name to avoid overwriting
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_name = f"{file_name.split('.')[0]}_{timestamp}.{file_name.split('.')[1]}"

    with open(file_name, "w") as f:
        # Convert the DataFrame to a list of dictionaries and save it as JSON
        json.dump(data.to_dict(orient="records"), f, indent=4) # orient="records" to get a list of dictionaries, one for each row in the DataFrame, and indent=4 for pretty printing (4 spaces for indentation)
