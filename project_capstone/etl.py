#------------------------------------
# MODULES AND LIBRARIES NECESSARIES

import configparser
import os
import logging
from functools import reduce
from datetime import timedelta, datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, DateType
from pyspark.sql.functions import unix_timestamp, to_date, dayofmonth, dayofweek, dayofyear, month, weekofyear, year
from pyspark.sql.functions import udf, col, lit, trim, split, first

#------------------------------------
# AWS CREDENTIALS, CONSTANT AND PARAMETERS
# Let's read the Access_key_id and Secret_key_id of the IAM user created with S3FullAccess role policy.
# Let's read all the constants and parameters needed to run this script

config = configparser.ConfigParser()
config.read('configuration/configuration.cfg')

# AWS Params
os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'KEY')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'SECRET')
PROJECT_BUCKET=config.get('AWS', 'S3_BUCKET')

# Data Params
PATH_I94_DATA=config.get('DATA', 'PATH_I94')
PATH_LOOKUP_DATA=config.get('DATA', 'PATH_LOOKUP')
PATH_TEMPERATURE_DATA=config.get('DATA', 'PATH_TEMPERATURE')
PATH_EXTERNAL_DATA=config.get('DATA', 'PATH_EXTERNAL_DATA')
PATH_AIRPORT_DATA=config.get('DATA', 'PATH_AIRPORT_DATA')
PATH_DEMOGRAPHIC_DATA=config.get('DATA', 'PATH_DEMOGRAPHIC_DATA')
INPUT_FORMAT_I94_DATA=config.get('DATA', 'FORMAT_I94')
FORMAT_CSV=config.get('DATA', 'FORMAT_CSV')
LOOKUP_AIRPORT=config.get('DATA', 'NAME_LOOKUP_AIRPORT')
LOOKUP_COUNTRIES=config.get('DATA', 'NAME_LOOKUP_COUNTRIES')
LOOKUP_MODE=config.get('DATA', 'NAME_LOOKUP_MODE')
LOOKUP_VISA=config.get('DATA', 'NAME_LOOKUP_VISA')   
WORLDCITIES=config.get('DATA', 'NAME_EXTERNAL_WORLDCITIES') 


#------------------------------------
# FUNCTIONS

def spark_session():
    """
    This function builds a Spark Session with some dependencies that will let us work with Datasets.
    """
    spark_session = SparkSession.builder.appName("Data Engineer Project Capstone")\
                                .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0")\
                                .enableHiveSupport()\
                                .getOrCreate()
     
    logging.info('Spark session created')

    return spark_session


def etl_i94_data(spark_session, path_data, format_data):
    """
    With this function, we read i94 data, enrich it with lookup tables and clean the result. Finally, we create a dimension table from the dates
    in the fact table and save both tables into AWS S3 bucket.
    @params
        spark_session: Spark Session previously created.
        path_data: Path of i94 fact table data.
        format_data: Format in which fact data appears.
    """
    # Extract
    i94_df_original = spark_session.read.load(path_data, format=format_data)
    
    lookup_airport_df = spark_session.read.load(f"{PATH_LOOKUP_DATA}/{LOOKUP_AIRPORT}", format=FORMAT_CSV, header=True)
    
    lookup_countries_df = spark_session.read.load(f"{PATH_LOOKUP_DATA}/{LOOKUP_COUNTRIES}", format=FORMAT_CSV, header=True)\
                                            .withColumn("country_code", col("country_code").cast(IntegerType()))
    
    lookup_mode_df = spark_session.read.load(f"{PATH_LOOKUP_DATA}/{LOOKUP_MODE}", format=FORMAT_CSV, header=True)\
                                       .withColumn("mode_code", col("mode_code").cast(IntegerType()))
    
    lookup_visa_df = spark_session.read.load(f"{PATH_LOOKUP_DATA}/{LOOKUP_VISA}", format=FORMAT_CSV, header=True)\
                                       .withColumn("visa_code", col("visa_code").cast(IntegerType()))


    logging.info('I94 data of January and lookups have been loaded into dataframes')

    
    # Transform
    columns = ['cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 'i94port', 'arrdate', 'i94mode', 'depdate', 'i94visa', 'gender', 'airline']
    i94_df = i94_df_original.select(columns)
    logging.info('Only useful columns of I94 dataframe selected')
    
    columns_to_cast_to_int = ['cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 'i94mode',  'i94visa', 'arrdate', 'depdate']
    i94_df_intcols = reduce(lambda df, column: df.withColumn(f"{column}", df[f"{column}"].cast(IntegerType())),
                            columns_to_cast_to_int,
                            i94_df)
    
    logging.info('Integer columns of I94 dataframe casted')

    columns_to_cast_to_date = ['arrdate', 'depdate']
    i94_df_datecols = reduce(lambda df, column: df.withColumn(f"{column}", date_sas_to_dateformat_udf(df[f"{column}"])),
                             columns_to_cast_to_date,
                             i94_df_intcols)
                         
    logging.info('Date columns of I94 dataframe casted')
    
    i94_df_airport = i94_df_datecols.join(lookup_airport_df, i94_df_datecols.i94port == lookup_airport_df.airport_code, how='left')\
                                    .withColumn('state_code', trim(col('state_code')))\
                                    .drop('airport_code')

    i94_df_citcountry = i94_df_airport.join(lookup_countries_df, i94_df_airport.i94res == lookup_countries_df.country_code, how='left')\
                                      .withColumnRenamed('country', 'born_country')\
                                      .drop('country_code')

    i94_df_rescountry = i94_df_citcountry.join(lookup_countries_df, i94_df_citcountry.i94res == lookup_countries_df.country_code, how='left')\
                                         .withColumnRenamed('country', 'residence_country')\
                                         .drop('country_code')
    
    
    i94_df_mode = i94_df_rescountry.join(lookup_mode_df, i94_df_rescountry.i94mode == lookup_mode_df.mode_code, how='left')\
                                   .drop('mode_code')

    i94_df_visa = i94_df_mode.join(lookup_visa_df, i94_df_mode.i94visa == lookup_visa_df.visa_code, how='left')\
                             .drop('visa_code')
    
    i94_df_output = i94_df_visa.withColumn('arrdate', to_date(unix_timestamp(col('arrdate'), 'dd/MM/yyyy').cast("timestamp")))\
                               .withColumn('depdate', to_date(unix_timestamp(col('depdate'), 'dd/MM/yyyy').cast("timestamp")))\
                               .dropDuplicates()
    
    logging.info('Lookup columns joined and duplicates dropped.')

    #-------------------------------------
    
    arrdate_df = i94_df_output.select(col('arrdate')).withColumnRenamed('arrdate', 'date')
    depdate_df = i94_df_output.select(col('depdate')).withColumnRenamed('depdate', 'date')
    
    dates_df = arrdate_df.union(depdate_df)\
                         .filter(col("date").isNotNull())\
                         .dropDuplicates()

    dates_dim = dates_df.withColumn('day', dayofmonth('date')) \
                        .withColumn('month', month('date')) \
                        .withColumn('year', year('date')) \
                        .withColumn('week', weekofyear('date')) \
                        .withColumn('weekday', dayofweek('date'))\
                        .withColumn('yearday', dayofyear('date'))
    
    logging.info(f'Date Dimension built')
    
    #Load

    i94_df_output.write.save(PROJECT_BUCKET + 'i94', format='csv', header=True, mode="overwrite")
    dates_dim.write.save(PROJECT_BUCKET + 'dates', format='csv', header=True, mode="overwrite")

    logging.info('I94 data and Date dimension loaded into AWS S3 bucket')


    
def etl_temperature(spark_session, path_data, format_data):
    """
    With this function, we read temperature data, enrich it with external data and create two tables. One with average temperature 
    by country and other with average temperature by US state. Both tables are loaded into AWS S3 bucket.
    @params
        spark_session: Spark Session previously created.
        path_data: Path of temperature data.
        format_data: Format in which temperature data appears.
    """
    
    #Extract
    
    orig_temp_df = spark_session.read.load(path_data, format_data, header=True)
    world_cities_df = spark_session.read.load(f"{PATH_EXTERNAL_DATA}/{WORLDCITIES}", FORMAT_CSV, header=True)
    
    #Transform
    
    us_cities_df = world_cities_df.select('country', 'city', 'admin_name').filter(col('country') == "United States")
    
    us_states_df = us_cities_df.groupBy('city').agg({'admin_name': 'first'}).withColumnRenamed('first(admin_name)', 'state')
    
    us_temp_cities_df = orig_temp_df.withColumn('date', to_date(unix_timestamp(col('dt'), 'yyyy-MM-dd').cast("timestamp")))\
                                    .filter(col('Country') == "United States") \
                                    .filter(col('AverageTemperature').isNotNull()) \
                                    .filter(col('date') > lit('2000-01-01')) \
                                    .select('date', 'City', 'AverageTemperature')\
    
    us_temp_states_df = us_temp_cities_df.join(us_states_df, us_temp_cities_df.City == us_states_df.city, how='left')\
                                         .groupBy('state').agg({'AverageTemperature': 'mean'})\
                                         .withColumnRenamed('avg(AverageTemperature)', 'average_temperature')
        
    world_temp_countries_df = orig_temp_df.withColumn('date', to_date(unix_timestamp(col('dt'), 'yyyy-MM-dd').cast("timestamp")))\
                                          .filter(col('AverageTemperature').isNotNull()) \
                                          .filter(col('date') > lit('2000-01-01')) \
                                          .groupBy('Country').agg({'AverageTemperature': 'mean'})\
                                          .withColumnRenamed('avg(AverageTemperature)', 'average_temperature')\
                                          .withColumnRenamed('Country', 'country')
    
    #Load
    
    us_temp_states_df.write.save(PROJECT_BUCKET + 'us_states_temperature', format='csv', header=True, mode="overwrite")
    world_temp_countries_df.write.save(PROJECT_BUCKET + 'world_temperature', format='csv', header=True, mode="overwrite")


    logging.info('Temperatures of different US states and countries saved into S3 bucket')
    
def etl_airport(spark_session, path_data, format_data):
    """
    With this function, we read airport data, clean it and load it into AWS S3 bucket.
    @params
        spark_session: Spark Session previously created.
        path_data: Path of airport data.
        format_data: Format in which airport data appears.
    """
    #Extract
    
    orig_airport_df = spark_session.read.load(path_data, format_data, header=True)
    
    #Transform
    
    cols_to_select = ['name', 'iso_country', 'iso_region', 'municipality', 'iata_code']
    
    airport_df_transf = orig_airport_df.select(cols_to_select)\
                                       .filter(col('iata_code').isNotNull())
    
    split_col = split(airport_df_transf['iso_region'], '-')
    
    us_airport_df = airport_df_transf.filter(col('iso_country') == "US")\
                                     .withColumn('state', split_col.getItem(1))\
                                     .drop(col('iso_region'))
    
    cols_to_clean = ['name', 'iso_country', 'state', 'municipality', 'iata_code']
    us_airport_out = reduce(lambda df, column: df.withColumn(f"{column}", replace_comma_by_space(df[f"{column}"])),
                            cols_to_clean,
                            us_airport_df)
    #Load
    
    us_airport_df.write.save(PROJECT_BUCKET + 'airport', format='csv', header=True, mode="overwrite",  sep=";")

    logging.info('Airport Data saved into S3 bucket')
    

    
def etl_demographic(spark_session, path_data, format_data):
    """
    With this function, we read demographic data, clean it, compute some cubes throught different dimmensions and load it into AWS S3 bucket.
    @params
        spark_session: Spark Session previously created.
        path_data: Path of demographic data.
        format_data: Format in which demographic data appears.
    """    
    #Extract
    
    orig_demog_df = spark_session.read.load(path_data, format_data, header=True, sep=";")
    
    #Transform
    
    groupby_cols = ['City', 'State', 'Median Age', 'Male Population', 'Female Population', 'Total Population', 
                    'Number of Veterans', 'Foreign-born', 'Average Household Size', 'State Code']
    
    demog_city_df = orig_demog_df.groupby(groupby_cols).pivot("Race").agg(first("Count"))
    
    demog_state_df = demog_city_df.groupby('State', 'State Code')\
                                  .agg({'Median Age':'mean', 'Male Population':'sum', 'Female Population':'sum', 
                                        'Total Population':'sum', 'Number of Veterans':'sum', 'Foreign-born':'sum', 
                                        'Average Household Size':'mean', 'American Indian and Alaska Native':'sum', 
                                        'Asian':'sum', 'Black or African-American':'sum', 'Hispanic or Latino':'sum', 'White':'sum'})\
                                  .withColumnRenamed('state','state')\
                                  .withColumnRenamed('State Code','state_code')\
                                  .withColumnRenamed('avg(Median Age)','median_age')\
                                  .withColumnRenamed('sum(Male Population)','male_population')\
                                  .withColumnRenamed('sum(Female Population)','female_population')\
                                  .withColumnRenamed('sum(Total Population)','total_population')\
                                  .withColumnRenamed('sum(Number of Veterans)','number_of_veterans')\
                                  .withColumnRenamed('sum(Foreign-born)','foreignborn')\
                                  .withColumnRenamed('avg(Average Household Size)','average_household_size')\
                                  .withColumnRenamed('sum(American Indian and Alaska Native)','american_indian_and_alaska_native')\
                                  .withColumnRenamed('sum(Asian)','asian')\
                                  .withColumnRenamed('sum(Black or African-American)','black_or_africanamerican')\
                                  .withColumnRenamed('sum(Hispanic or Latino)','hispanic_latino')\
                                  .withColumnRenamed('sum(White)','white')

    #Load

    demog_state_df.write.save(PROJECT_BUCKET + 'demographic', format='csv', header=True, mode="overwrite")

    logging.info('Demographic data saved into S3 bucket')    
    
    


#------------------------------------
# User Defined Functions (UDFs)

date_sas_to_dateformat_udf = udf(lambda day: None if day is None else (timedelta(days=day) + datetime(1960, 1, 1)).strftime('%d/%m/%Y'))
replace_comma_by_space = udf(lambda x : str(x).replace(',', ' '))


#------------------------------------
# MAIN

if __name__ == "__main__":
    

    logging.basicConfig(filename='app.log', filemode='a', level=logging.INFO, format='%(levelname)s | %(asctime)s | %(message)s', datefmt='%d-%b-%y %H:%M:%S') 
    
    logging.info('-----------------------------------------------------')
    logging.info('Start of etl.py script')
    logging.info('-----------------------------------------------------')

    spark_session = spark_session()

    etl_i94_data(spark_session, PATH_I94_DATA, INPUT_FORMAT_I94_DATA)
    etl_temperature(spark_session, PATH_TEMPERATURE_DATA, FORMAT_CSV) 
    etl_airport(spark_session, PATH_AIRPORT_DATA, FORMAT_CSV)
    etl_demographic(spark_session, PATH_DEMOGRAPHIC_DATA, FORMAT_CSV)
    
    logging.info('-----------------------------------------------------')
    logging.info('End of etl.py script')
    logging.info('-----------------------------------------------------')