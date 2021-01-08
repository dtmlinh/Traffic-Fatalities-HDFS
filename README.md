Name: Linh Dinh

## Project Description:
    A project to look at the fatality rates of traffic accidents in the US and which factors might impact these rates. This project utitlizes several big data tools: AWS EMR cluster, HDFS, Hive, Spark, Hbase. 

    Contributors: Linh Dinh

## Data Sources and Project Objectives
    1. Bureau of transportation data:
    - Actual fatal accidents data for 2016-2018
    - Sampling non-fatal accidents data for 2016-2018 (NOT COMPLETE DATA COVERAGE)
    
    I used these 2 data sources to try a Random Forest model predicting "fatal cases". I then identified a few factors 
    that the Random Forest model (see `4. ML_spark.scala`) suggests are "important": 
        - Weather
        - Ligh condition: day vs. night
        - Occur at junction or not
        - Week day
        - Hour of Day
        - Etc.

    2. Kaggle data for total US self-reported accidents data for 2016-2020: https://www.kaggle.com/sobhanmoosavi/us-accidents
    I used this Kaggle dataset to calculate the fatality rate (number of fatal accidents/number of total accidents) because the sampling non-fatal accidents data described above are not complete data coverage (i.e., randomly sample data from selected number of locations). I leveraged this Kaggle dataset for my denominator in the fatality rate calculation. 

## Usage
    The final output shows by State and Year: 
    - The fatality rate for serveral interesting conditions that might influence whether an accident is fatal or not: day vs. night time, at a 
    junction, weather, etc.
    - I then also included a few interesting data such as: 
    + average number of minutes injured persons arrive at the hospital
    + average number of hospitals within a 10 mile radius of the accident
    + share of state spending on highway investments and health investments

    ![](Transportation-Analyses.gif)

## Structure of the software
    - `0. ingest_data.sh`: Codes to ingest needed data
    - `1. create_truth_tables.hql`: HQL queries to create ground truth tables in Hive
    - `2. batch_layer.scala`: Spark codes to create batch layer tables in Hive
    - `3. create_hbase_tables.hql`: Codes to create hbase tables for serving layer
    - `4. ML_spark.scala`: ML codes to train a random forest model
    - folder `app`: Java and HTML codes to deploy app on AWS instance

## Deployed app:
    - Static: http://ec2-3-15-219-66.us-east-2.compute.amazonaws.com:3199/accidents.html
    - LB: http://MPCS53014-LoadBalancer-217964685.us-east-2.elb.amazonaws.com:3199/accidents.html

