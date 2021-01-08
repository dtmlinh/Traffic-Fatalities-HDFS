Name: Linh Dinh

'''Codes to ingest relevant data'''


'''HOSPITALS DATA'''
'''Curl the raw data directly into S3 bucket, then copy this to HDFS cluster and unzip'''
curl https://prod-hub-indexer.s3.amazonaws.com/files/6ac5e325468c4cb9b905f1728d6fbf0f/0/full/3857/6ac5e325468c4cb9b905f1728d6fbf0f_0_full_3857.csv | aws s3 cp - s3://traffic-fatalities-hdfs/Hospitals.csv.gz
s3-dist-cp --src=s3://traffic-fatalities-hdfs/Hospitals.csv.gz --dest=hdfs:///traffic-fatalities-hdfs/Hospitals/Hospitals
hdfs dfs -cat hdfs:///traffic-fatalities-hdfs/Hospitals/Hospitals.csv.gz | gzip -d | hdfs dfs -put - hdfs:///traffic-fatalities-hdfs/Hospitals/Hospitals/Hospitals.csv


'''GEO MAPPING'''
'''Download the raw data locally, upload this onto S3 bucket, then copy this to HDFS cluster'''
s3-dist-cp --src=s3://traffic-fatalities-hdfs/FRPP_GLC_-_United_StatesSep292020.csv --dest=hdfs:///traffic-fatalities-hdfs/GEO/GEO


'''ACCIDENTS DATA'''
'''Curl the raw data directly into S3 bucket, then copy this to HDFS cluster and unzip'''
curl 'https://storage.googleapis.com/kaggle-data-sets/199387/1319582/compressed/US_Accidents_June20.csv.gz?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20210108%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20210108T072930Z&X-Goog-Expires=259199&X-Goog-SignedHeaders=host&X-Goog-Signature=73e25eab7e6bbadd7e52c9cd9b6618600757d7364e72031188fe0d7419a4195c71712fe4f4c3540347d93cede80f3e8d9fa0f4f14f2a34748c0b2e7fa2a8538e9ee334c61ee4713a368c3f3c23707a98d4cf2bab8e4026366c0a038ecedf42b0f3d6fced3570f3e469f7448755fe37d61357ff0100cca6b807906abba28730ca036b6affcb3a3446cb0616bdce6d2d0dfa42d66a240d46ed4fe8512184dc6ae2a78333f4316b46b571f35951d7ce3c9dce09613f18cadacae6da945d58222f9b2815e8ccde51e8d725f03176a9535defec02e78ad0c26c4a0f0b0d898fc2613b2c1135261746e5d8b72b26060fc247d10991e3acd8f6963327909d56282802fe' | aws s3 cp - s3://traffic-fatalities-hdfs/US_Accidents_June20.csv.gz 
s3-dist-cp --src=s3://traffic-fatalities-hdfs/US_Accidents_June20.csv.gz --dest=hdfs:///traffic-fatalities-hdfs/accidents/accidents
hdfs dfs -cat hdfs:///traffic-fatalities-hdfs/accidents/US_Accidents_June20.csv.gz | gzip -d | hdfs dfs -put - hdfs:///traffic-fatalities-hdfs/accidents/accidents/US_Accidents_June20.csv


'''FATALITIES DATA'''
'''Curl the raw data directly into HDFS master node and unzip, then copy this to HDFS cluster'''
mkdir traffic-fatalities-hdfs
cd traffic-fatalities-hdfs
mkdir FARS2016
mkdir FARS2017
mkdir FARS2018

curl 'https://www.nhtsa.gov/filebrowser/download/122001' > FARS2016/FARS2016.zip
curl 'https://www.nhtsa.gov/filebrowser/download/118801' > FARS2017/FARS2017.zip
curl 'https://www.nhtsa.gov/filebrowser/download/176791' > FARS2018/FARS2018.zip

unzip FARS2016.zip
unzip FARS2017.zip
unzip FARS2018.zip

cat FARS2016/accident.CSV | hdfs dfs -put - hdfs:///traffic-fatalities-hdfs/FARS/FARS/FARS2016.csv
cat FARS2017/accident.CSV | hdfs dfs -put - hdfs:///traffic-fatalities-hdfs/FARS/FARS/FARS2017.csv
cat FARS2018/accident.CSV | hdfs dfs -put - hdfs:///traffic-fatalities-hdfs/FARS/FARS/FARS2018.csv


'''NON-FATALITIES DATA'''
'''Curl the raw data directly into HDFS master node and unzip, then copy this to HDFS cluster'''
NON-FATALITIES DATA
mkdir CRSS2016
mkdir CRSS2017
mkdir CRSS2018

curl 'https://www.nhtsa.gov/filebrowser/download/117931' > CRSS2016/CRSS2016.zip
curl 'https://www.nhtsa.gov/filebrowser/download/123631' > CRSS2017/CRSS2017.zip
curl 'https://www.nhtsa.gov/filebrowser/download/176936' > CRSS2018/CRSS2018.zip

unzip CRSS2016.zip
unzip CRSS2017.zip
unzip CRSS2018.zip

cat CRSS2016/ACCIDENT.CSV | hdfs dfs -put - hdfs:///traffic-fatalities-hdfs/CRSS/CRSS/CRSS2016.csv
cat CRSS2017/ACCIDENT.CSV | hdfs dfs -put - hdfs:///traffic-fatalities-hdfs/CRSS/CRSS/CRSS2017.csv
cat CRSS2018/ACCIDENT.csv | hdfs dfs -put - hdfs:///traffic-fatalities-hdfs/CRSS/CRSS/CRSS2018.csv


'''FINANCE DATA'''
'''Download the raw data locally, upload this onto S3 bucket, then copy this to HDFS cluster'''
FINANCES DATA
Query from: https://data.census.gov/cedsci/table?q=government%20finance&tid=GOVSTIMESERIES.GS00SG01&hidePreview=true
s3-dist-cp --src=s3://traffic-fatalities-hdfs/GOVSTIMESERIES.GS00SG01-2020-12-10T032546.csv --dest=hdfs:///traffic-fatalities-hdfs/gov_spending/gov_spending/

