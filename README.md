# infovision_project

Data Acquisition:- 
    API Glue job:- Script api_incremental will be the Glue job which will fetch the daily incremental data from the APIs. In this cript we have to provide the input argument such as secret name, api name. Secret name is used top fetch the secret token to connect to the API.
    This glue job will run daily and after receiving the response from teh API. Response will be loaded in the text file in the s3 location. This script will also removes the unncessary columns.


Data Ingestion:- Data Ingestion will have 2 script landing to bronze and bronze to silver. Once acquisition jobs put the file in the landing zone in the text format. Databricks workflow will picks this file and read as a json file and add the audit columns to it and place in the bronze layer. Bronze layer will have all the historical data in the same format as it was received.
Then next script of bronze to silver will run where this will add only the latest snspshot of the data in the silver layer. It will merge the data frame and gets the latest sanpshot of the data.

Airflow:- All the orchestration will be done through the Airflow.


Unit Testing:-
1) Establish the API connection successfully.
2) Proper error response handling through the API.
3) Backoff strategy to retry to establist the connection once not establisted. To have certain number of retries.
4) Once established the connection then getting the proper response as expected through the API.
5) Placing the response file s3 landing zone.
6) Landing to bronze and bronze to silver script works completely fine.
7) Data is not duplicated in the silver layer. Silver layer should have latest snapshot of the data.
8) Glue Job and Databricks workflow should run as scheduled in the Airflow.
9) Data at the source should be equal to Data at sink.
10) Errored record should go in the rescued column. 

