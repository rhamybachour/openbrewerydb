from pyspark.sql import types as T

class BronzeSchema():
    
    BREWERY_SCHEMA = T.StructType([
        T.StructField("id", T.StringType()),
        T.StructField("name", T.StringType()),
        T.StructField("brewery_type", T.StringType()),
        T.StructField("address_1", T.StringType()),
        T.StructField("address_2", T.StringType()),
        T.StructField("address_3", T.StringType()),
        T.StructField("city", T.StringType()),
        T.StructField("state_province", T.StringType()),
        T.StructField("postal_code", T.StringType()),
        T.StructField("country", T.IntegerType()),
        T.StructField("longitude", T.StringType()),
        T.StructField("latitude", T.StringType()),
        T.StructField("phone", T.StringType()),
        T.StructField("website_url", T.StringType()),
        T.StructField("state", T.IntegerType()),
        T.StructField("street", T.StringType()),
    ])

class SilverSchema():
    
    BREWERY_SCHEMA = T.StructType([
        T.StructField("id", T.StringType()),
        T.StructField("name", T.StringType()),
        T.StructField("brewery_type", T.StringType()),
        T.StructField("address_1", T.StringType()),
        T.StructField("address_2", T.StringType()),
        T.StructField("address_3", T.StringType()),
        T.StructField("city", T.StringType()),
        T.StructField("state_province", T.StringType()),
        T.StructField("postal_code", T.StringType()),
        T.StructField("country", T.IntegerType()),
        T.StructField("longitude", T.StringType()),
        T.StructField("latitude", T.StringType()),
        T.StructField("phone", T.StringType()),
        T.StructField("website_url", T.StringType()),
        T.StructField("state", T.IntegerType()),
        T.StructField("street", T.StringType()),
    ])

class GoldSchema():
    pass