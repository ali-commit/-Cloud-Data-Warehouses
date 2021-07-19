with sql_queries I've built all tables such as staging tables 
then dimetional tables 

with create_tables.py file we imported data from other files and run them using functioms.


with dwh.cfg we passed credential and non credential to set up etls
 with etl.py file excutie the function to build and processes commands 
 
 etl files has function that build pipline from s3 to redshift.
 
requirement of this project is using python to run script and amzon redshit to store dimentional model.


summary of the project 

is to build data warehouse using redshift cluster by taking the dataset from S3 buket and and trasform your data into final tables.




 
 
 
 
 