from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession


# Flatten array of structs and structs
def nested_json_flatten(df):
    # compute Complex Fields (Lists and Structs) in Schema
    cmplx_fields = dict([(field.name, field.dataType)
                           for field in df.schema.fields
                           if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
    while len(cmplx_fields) != 0:
        col_nme = list(cmplx_fields.keys())[0]
        print("Processing :" + col_nme + " Type : " + str(type(cmplx_fields[col_nme])))

        # if StructType then convert all sub element to columns i.e. flatten structs
        if (type(cmplx_fields[col_nme]) == StructType):
            expanded = [col(col_nme + '.' + k).alias(col_nme + '-' + k) for k in
                        [n.name for n in cmplx_fields[col_nme]]]
            df = df.select("*", *expanded).drop(col_nme)

        # if ArrayType then add the Array Elements as Rows using the explode function i.e. explode Arrays
        elif (type(cmplx_fields[col_nme]) == ArrayType):
            df = df.withColumn(col_nme, explode_outer(col_nme))

        # recompute remaining Complex Fields in Schema
        cmplx_fields = dict([(field.name, field.dataType)
                               for field in df.schema.fields
                               if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
    return df


# creating spark session object using builder method
spark = SparkSession \
    .builder \
    .master("local") \
    .appName("DataChallenge") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

if __name__ == "__main__":
# reading json file in spark df
    df_readJson = spark.read\
        .option("multiline", "true")\
        .json("C:///Users//himanssharma//Downloads//DE Assignment//ps_us_detailed-2021-03-24-01-16-1616548560.json")

    print("Json Schema and Data Before Flattening")
    df_readJson.printSchema()
    df_readJson.show(100, False)

# storing flattened json in df_result
    print("Json Schema and Data After Flattening")
    df_result = nested_json_flatten(df_readJson)
    df_result.printSchema()
    df_result.show(1000, False)

#The data in json is flatten and is availabe to query upon in 'df_result'
#A temp view can be created on 'df_result' and sql queries can be run like any normal table.

# saving flattened df output as a csv table in local path
    df_result.write\
        .option("header", True)\
        .mode("append")\
        .csv("C:///Users//himanssharma//Downloads//DE Assignment//table.csv")



