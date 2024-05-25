The order to run the scritps are:
```
download_aneel_companies.py
make_bronze_aneel_dataset_parquet.py
make_bronze_aneel_dataset_postgres.py
```

This is because we first download the scripts from the internet, then we save it into parquet, and then we read the parquet and save ir on the database.
We follow this process because if we try to save as parquet and then to the database, the memory load surpass the notebook RAM, or overload the CPU.