### CREATED ON - July 02 2019
### MODIFIED ON - July 02 2019
### DEVELOPER - PRATEEK GURJAR

This project covers the entire ETL process for our client Sparkify. Sparkify is a service that provides on demand music streaming for its customers. In the project we started with creating a dimensional model with Songplays as Fact Table and Songs, Artists, Time and Users as Dimension Tables. The dimensional model is basically as STAR Schema. Our data is located in S3 buckets, we write ETL logic to move data from S3 buckets into a Staging Area and load data from Staging into Dimension and Fact Tables on Redshift. 

### Technology Stack
For building our Data Warehouse we used a columnar storage by AWS called Redshift for extracting and transforming data.
For loading our data into the data warehouse we used COPY commands in conjuction with Python using the Psycopg2 library

### Data Sources
Data is stored in S3 buckets and we use COPY command to load data from S3 into Staging area
1. /data/songs 
From the songs directory we created Songs and Artists dimension tables 
2. /data/logs
From the logs directory we created Time and Users Dimension tables. We used pandas transformations for creating Time Dimension tables    # convert timestamp column to datetime

Fact Table - We used song_id and artist_id from the song and artist dimension tables in conjuction with other columns from logs dataset to create fact table.

### Data Warehouse Build
create_tables.py consists of SQL required for building the data-warehouse. It imports Drop and Create Statements from sql.queries.py and runs them in a particular sequence. 

Following are the steps to build the data-warehouse
1. In the project workspace, open a new terminal
2. Go in project directory and type following command
    python create_tables.py

### ETL Pipeline
etl.py file consists of Python functions for loading data from S3 buckets into staging and inserting data from Staging into dimension and fact tables. 

Following are the steps to run the ETL pipeline
1. In the project workspace, open a new terminal
2. Go in project directory and type following command
    python etl.py

### Future Enhancements
Based on dimension and fact tables we could now build data marts for Analytics and Reporting purposed. Some basic examples of report would be
- No of Paid and Free members from a location
- Artist and Song popularity by location
- Top Artists or Songs by Month/Year/Week


