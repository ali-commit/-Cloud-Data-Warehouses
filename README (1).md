with sql_queries I've built all tables such as staging tables 
then dimetional tables 

with create_tables.py file we imported data from other files and run them using functioms.


with dwh.cfg we passed credential and non credential to set up etls
 with etl.py file excutie the function to build and processes commands 
 
 etl files has function that build pipline from s3 to redshift 