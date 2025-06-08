/*
==============================================================================
DDL script: Bronze layer
==============================================================================
  This script defines the schema and table structures for the 
  Bronze Layer of a data warehouse in PostgreSQL.

  The bronze layer stores raw, ingested data and serves as the 
  initial landing zone for source system extracts.

  Tables Created:
    • crm_cust_info         – Customer master data
    • crm_prd_info          – Product master data
    • crm_sales_details     – Sales transaction records
    • erp_cust_az12         – ERP customer data
    • erp_loc_a101          – Location information
    • erp_px_cat_g1v2       – Pricing category reference

  Each table is created inside the `bronze` schema, with existing 
  tables dropped before recreation to ensure a clean setup.
==============================================================================
*/


DROP TABLE IF EXISTS bronze.crm_cust_info;
-- Create crm_cust_info table in bronze schema
CREATE TABLE bronze.crm_cust_info (
    cst_id INT, 
    cst_key VARCHAR(50), 
    cst_firstname VARCHAR(50), 
    cst_lastname VARCHAR(50), 
    cst_marital_status VARCHAR(50), 
    cst_gndr VARCHAR(50), 
    cst_create_date DATE
);


DROP TABLE IF EXISTS bronze.crm_prd_info;
-- Create crm_prd_info table in bronze schema
CREATE TABLE bronze.crm_prd_info(
	prd_id INT, 
	prd_key VARCHAR(50), 
	prd_nm VARCHAR(50), 
	prd_cost INT, 
	prd_line VARCHAR(10), 
	prd_start_dt DATE,
	prd_end_dt DATE
);


DROP TABLE IF EXISTS bronze.crm_sales_details;
-- Create crm_sales_details table in bronze schema
CREATE TABLE bronze.crm_sales_details(
	sls_ord_num VARCHAR(50), 
	sls_prd_key VARCHAR(50), 
	sls_cust_id INT, 
	sls_order_dt INT, 
	sls_ship_dt INT, 
	sls_due_dt INT, 
	sls_sales INT, 
	sls_quantity INT, 
	sls_price INT	
);

DROP TABLE IF EXISTS bronze.erp_cust_az12;
-- Create erp_cust_az12 table in bronze schema
CREATE TABLE bronze.erp_cust_az12(
	cid VARCHAR(50),
	bdate DATE, 
	gen VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_loc_a101;
-- Create erp_loc_a101 table in bronze schema
CREATE TABLE bronze.erp_loc_a101(
	cid VARCHAR(50),
	cntry VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
-- Create erp_px_cat_g1v2 table in bronze schema
CREATE TABLE bronze.erp_px_cat_g1v2(
	id VARCHAR(50), 
	cat VARCHAR(50), 
	subcat VARCHAR(50), 
	maintenance VARCHAR(50)
);
