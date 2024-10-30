import requests
import json
import datetime as dt
import boto3

from pyspark.dbutils import DBUtils
from py4j.protocol import Py4JJavaError
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable

class Base():
    
    def __init__(self, path_prefix=None, endpoint=None, context=None):
        
        # Set initial variables with the current date and time, adjusted to timezone
        self.search_datetime = (dt.datetime.now() - dt.timedelta(hours=3)).replace(microsecond=0)
        self.year = dt.datetime.today().strftime("%Y")
        self.month = dt.datetime.today().strftime("%-m")
        self.day = dt.datetime.today().strftime("%-d")
        
        # Set the root directory for saving JSON files
        self.path_prefix = path_prefix
        self.endpoint = endpoint
        self.context = context

    def sanitize_string(self, value):
    
        # Ensure value is a valid non-empty string; if not, return a blank string
        if not isinstance(value, str):
            return value

        if value.strip() == '':
            return ''

        if value == 'None':
            return ''

        return value

    def handler_response(self, response):

        # Return JSON data if status code is 200, otherwise return an empty list
        if response.status_code == 200:
            return response.json()
        else:
            return []

    def get_widgets(self, key, default_value=''):
    
        # Retrieve or initialize widget input and sanitize it
        try:
            DBUtils().widgets.text(key, str(default_value))
            value = DBUtils().widgets.get(key)

            return self.sanitize_string(value)

        # Handle error if the widget is not defined
        except Py4JJavaError as e:
            if 'InputWidgetNotDefined' in str(e):
                return default_value
            raise e

    def get_path(self, layer, format="raw", endpoint=None, context=None):

        # Use given or default endpoint/context and get the specific object data
        endpoint = endpoint or self.endpoint
        context = context or self.context

        # Generate the path based on the layer (bronze, silver, etc.) and endpoint/context
        if layer == "bronze":
            return f"/{layer}-layer/{endpoint}/{format}-data/{context}"
        else:
            return f"/{layer}-layer/{endpoint}/{context}"
        
    def save_object(self, endpoint, context, page, object):
        
        # Save JSON file and return a list with the status of the save operation
        path = f"{self.path_prefix}{self.get_path('bronze', endpoint=endpoint, context=context)}/year={self.year}/month={self.month}/day={self.day}/{object['id']}.json"
        if DBUtils().fs.put(path, json.dumps(object, ensure_ascii=False), True):
            return [object['id'], page, "Loaded"]
        else:
            return [object['id'], page, "Not Loaded"]
        
    def optimize_vacuum_delta(self, spark, delta_path):
        
        # Load the Delta table from the specified path using the DeltaTable class
        delta_table = DeltaTable.forPath(spark, delta_path)
        
        # Perform table optimization to improve query performance by compacting small files
        delta_table.optimize().executeCompaction()

        # Clean up old files and remove unreferenced data from the table to reclaim storage space, default is 7 days (168 hours)
        delta_table.vacuum()
        return 'OK'
        
    def save_merged_delta(self, spark, delta_path, delta_temp, condition):

        try:

            if 'delta' not in locals():
                try:
                    delta = spark.read.load(delta_path)
                except:
                    delta = spark.createDataFrame([], delta_temp.schema)

            if isinstance(condition, tuple) or isinstance(condition, list):
                columns = [col for col in condition if col in delta.columns]
                query = " AND ".join([f"delta.{col} <=> delta_temp.{col}" for col in columns])
            else:
                columns = [col for col in delta.columns if col in delta_temp.columns]
                query = " AND ".join([f"delta.{col} <=> delta_temp.{col}" for col in columns])
                
            delta_table = DeltaTable.forPath(spark, delta_path)
            (delta_table.alias("delta")
                .merge(delta_temp.alias("delta_temp"), query)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            self.optimize_vacuum_delta(spark, delta_path)

            return 'Saved'
        
        except AnalysisException as e:
            mensagem = str(e)
            if "is not a Delta table" in mensagem:
                delta_temp.write.format("delta").mode("overwrite").option("path", delta_path).save()
                self.optimize_vacuum_delta(spark, delta_path)
                return 'Saved'
            else:
                raise e

    def save_overwrite_delta(self, spark, delta_path, delta_table, merge_schema=False, overwrite_schema=False):

        options = {
            'mergeSchema': merge_schema,
            'overwriteSchema': overwrite_schema
        }

        delta_table.write.options(**options).mode('overwrite').format('delta').option('path', delta_path).save()
        self.optimize_vacuum_delta(spark, delta_path)
        return 'Saved'

class API(Base):
    def __init__(self, endpoint, context, path_prefix):
        
        # Initialize parent class to inherit attributes
        super().__init__(path_prefix)
        
        # Define endpoint and context specific to this API instance
        self.endpoint = endpoint
        self.context = context

        # Set the base URL for API requests
        self.base_url = f"https://{endpoint}/{context}"

    def request(self, method, headers={}, params={}, payload={}, timeout=30):
        
        # Handle only GET requests for now (extendable to other methods)
        if method == "GET":
            response = requests.request(method, self.base_url, headers=headers, params=params, timeout=timeout)
            return response    

    def request_by_id(self, method, object_id=None, headers={}, params={}, payload={}, timeout=30):
        
        # Check if object_id is provided; if not, raise an error
        if method == "GET":
            if not object_id:
                raise ValueError("Please, provide a valid object id.")
            else:
                # Make GET request for a specific object by its ID
                response = requests.request(method, f"{self.base_url}/{object_id}", headers=headers, params=params, timeout=timeout)
                return response
        else:
            raise ValueError("Method not implemented.")


class OpenBreweryDB(API):
    
    def __init__(self, endpoint, context, path_prefix):
        
        # Initialize the parent API class
        super().__init__(endpoint, context, path_prefix)

    def list(self):
       
        # Initialize parameters to retrieve a list of objects with pagination
        params = {
            "per_page": 200,
            "page": 1
        }
        
        # Make an initial GET request and handle the response
        response = self.request(method="GET", params=params)
        breweries = self.handler_response(response)
        breweries_list = []

        # Loop through paginated results until no more data is found
        while len(breweries) > 0:
            
            # Save metadata to use into future requests
            breweries_list.extend([[self.endpoint, self.context, params["page"], self.search_datetime, object_id["id"], "Not Loaded"] for object_id in breweries])
            
            # Move to the next page and repeat the request/response handling
            params["page"] += 1
            response = self.request(method="GET", params=params)
            breweries = self.handler_response(response)

        # Return the list of breweries or data logs
        return breweries_list

    def get(self, object_id, page, endpoint=None, context=None):
        
        # Use given or default endpoint/context and get the specific object data
        endpoint = endpoint or self.endpoint
        context = context or self.context

        return_list = []

        # Make a GET request for a single object by ID
        return_object = self.handler_response(self.request_by_id("GET", object_id))

        # Check if the data is a dictionary; if so, save it and return the status of the save operation
        if isinstance(return_object, dict):
            return self.save_object(endpoint, context, page, return_object)
        else:
            raise TypeError("The type of Object it is not a Dictionary")
