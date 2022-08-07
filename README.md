# Spark Project - NYC Taxi Data

----------------------------------------------------------------------------------------------------------------------
### Scope the Project and Gather Data

#### Scope
This project will integrate NYC Taxi Trip Data with Taxi Zone Lookup Data to create a dataset that can be used for descriptive and predictive analysis. For example, to predict the number of trips per day for a given taxi zone.

#### Data Sets 
* [Yellow and Green Taxi Trip Records for 2019, 2020, 2021](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
* [Taxi Zone Maps and Lookup Tables](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
* [Taxi Zone Lookup Data](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)


#### Tools
* AWS S3: data storage
* AWS EMR: for distributed data processing
* Python: for data processing
* Pandas: exploratory data analysis on small dataset
* PySpark: data processing on large dataset


#### Data Source 

| Data Set | Format | Description |
| ---      | ---    | ---         |
|[TLC Trip Record Data](https://travel.trade.gov/research/reports/i94/historical/2016.html)| Parquet | Yellow and green taxi trip records include fields capturing pick-up and drop-off dates/times, pick-up and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts. The data used in the attached datasets were collected and provided to the NYC Taxi and Limousine Commission (TLC) by technology providers authorized under the Taxicab & Livery Passenger Enhancement Programs (TPEP/LPEP).
|[Taxi Zone Maps and Lookup Tables](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)| CSV | This dataset contains dimension information about the trip records that are being collected. The data used in the attached datasets were collected and provided to the NYC Taxi and Limousine Commission (TLC) by technology providers authorized under the Taxicab & Livery Passenger Enhancement Programs (TPEP/LPEP).|


### Data Model
The data model is a collection fact table data and dimension information about the trip records. 

#### Mapping Out Data Pipelines
1. The data is extracted from the data source, and stored in AWS S3 in Parquet and CSV formats.
2. The data is loaded into a dataframe.
3. The data is cleaned and transformed.
4. The cleaned dataset is returned as a denormalized dataframe.


#### Data dictionary
1. [Yellow Taxi](https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf) for the data dictionary.
2. [Green Taxi](https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf) for the data dictionary.

### How to use this project
Installation via `requirements.txt`:
 ```pycon
   $ git clone https://github.com/ofili/nyc-taxi-data.git
   $ cd nyc-taxi-data
   $ py -3 -m venv venv
   $ source venv/bin/activate
   $ pipenv install -r requirements.txt
   $ python3 main.py
```

