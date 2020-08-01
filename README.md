
# Project Overview
This project migrates local option price to AWS redshift database for further analysis


## Design
### Relational Database
* Fact Table
    * fact_options_transation
* Dimension Table
    * dim_contracts
    * dim_tickers

### Workflow
![Image of Workflow](https://www.dropbox.com/s/hkjtdrfnmd62c6d/option_data_flowchart.png?raw=1)


## Files
* src
    * sql_queries.py
        * All SQL quries used for ETL are stored here
    * create_table.py
        * Create or recreate tables and related objects
    * etl.py
        * Used to ETL full dataset from S3 to Redshift
* jupyter_notebook_test
    * etl.ipynb
        * Jupyter Notebook used to develop ETL.
    * create_table.ipynb
        * Jupyter Notebook used to test data creating process.
        

## ER Diagram
![Image of ER Diagram](https://www.dropbox.com/s/ryyhumkv1abu2yt/option_price.jpeg?raw=1)
