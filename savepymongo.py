import pandas as pd
from pymongo import MongoClient


def write_df_to_mongoDB(  
    dataframe,
    database_name, 
    collection_name
):
    """
    Write a Pandas DataFrame to MongoDB.

    Parameters:
    dataframe (pd.DataFrame): The DataFrame to be written.
    database_name (str): The name of the database.
    collection_name (str): The name of the collection.
    server (str): The server address (default is localhost).
    port (int): The port number (default is 27017).
    """

    # Create a MongoDB client
    client = MongoClient("mongodb://localhost:27017/")


    # Select the database
    db = client[database_name]

    # Select the collection
    collection = db[collection_name]

    # Convert DataFrame to dictionary
    data = dataframe.to_dict(orient='records')

    try:
        # Database operations here...
        for record in data:
            identifier =  {'uuid': record['uuid']}
            collection.update_one(identifier, {'$set': record}, upsert=True)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Ensure the client is closed even if an error occurs
        client.close()


if __name__ == '__main__':
    df = pd.read_csv(r'c:\Users\grc\algoorecordslfattestnewestbiggest10172023.csv')
    write_df_to_mongoDB(df,'local','recs')