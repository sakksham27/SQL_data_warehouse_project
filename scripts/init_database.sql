/*
==================================================
Create Database and Schemas
==================================================

Initialize DB: 

  This script initializes the foundational structure for the Data Warehouse Project.
  It creates the primary database "DataWarehouse", 
  which contains three structured schemas:

    • bronze – stores raw, ingested data
    • silver – holds cleaned and validated data
    • gold   – contains business-ready, aggregated data

  These layers reflect the modern data lakehouse architecture
  and enable efficient data transformation and analytics.

*/



-- Create the database: "DataWarehouse" 
CREATE DATABASE DataWarehouse;

-- Create bronze, silver, gold schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

