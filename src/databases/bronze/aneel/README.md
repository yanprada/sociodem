The order to run the scritps are:
```
download_aneel_companies.py
make_bronze_aneel_dataset.py
```

This is because we first download the scripts from the internet, then we save it into parquet and in postgres database.

Under the infrastructure schema in postrgres database, we will have five tables. 
1- aneel_companies_id: This table refers to the id stablished by aneel for each energy company in Brazil. We use this id to download the data from the aneel website
2- 