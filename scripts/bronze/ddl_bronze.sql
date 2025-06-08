/*
============================================================================
DDL script: Bronze Schema
============================================================================

	This script defines a PostgreSQL stored function bronze.load_bronze()
    responsible for loading raw data into the bronze layer of a data warehouse.
  
  	For each table in the bronze schema, the function:
	    • Truncates existing data
	    • Loads fresh data from a corresponding CSV file using COPY
	    • Logs row counts and duration per table
	    • Provides structured progress output using RAISE NOTICE

============================================================================

*/



CREATE OR REPLACE FUNCTION bronze.load_bronze()
RETURNS void 
LANGUAGE plpgsql
AS $$
DECLARE
    row_count INTEGER;
	start_time TIMESTAMP;
    end_time   TIMESTAMP;
	total_start_time TIMESTAMP;
	total_end_time TIMESTAMP;
	
	
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Loading Bronze Layer...';
    RAISE NOTICE '========================================';
	total_start_time := clock_timestamp();
    -- Load bronze.crm_cust_info
    RAISE NOTICE 'Processing table: bronze.crm_cust_info';
	start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_cust_info;
    COPY bronze.crm_cust_info
    FROM '/Library/PostgreSQL/17/data/csv_import/cust_info.csv'
    WITH (FORMAT csv, HEADER, DELIMITER ',');
	end_time := clock_timestamp();
    GET DIAGNOSTICS row_count = ROW_COUNT;
    RAISE NOTICE '  ✓ Loaded % rows into bronze.crm_cust_info', row_count;
	RAISE NOTICE ' ⏱ Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- Load bronze.crm_prd_info
    RAISE NOTICE 'Processing table: bronze.crm_prd_info';
	start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info
    FROM '/Library/PostgreSQL/17/data/csv_import/prd_info.csv'
    WITH (FORMAT csv, HEADER, DELIMITER ',');
	end_time := clock_timestamp();
    GET DIAGNOSTICS row_count = ROW_COUNT;
    RAISE NOTICE '  ✓ Loaded % rows into bronze.crm_prd_info', row_count;
	RAISE NOTICE ' ⏱ Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- Load bronze.crm_sales_details
    RAISE NOTICE 'Processing table: bronze.crm_sales_details';
	start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details
    FROM '/Library/PostgreSQL/17/data/csv_import/sales_details.csv'
    WITH (FORMAT csv, HEADER, DELIMITER ',');
	end_time := clock_timestamp();
    GET DIAGNOSTICS row_count = ROW_COUNT;
    RAISE NOTICE '  ✓ Loaded % rows into bronze.crm_sales_details', row_count;
	RAISE NOTICE ' ⏱ Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- Load bronze.erp_cust_az12
    RAISE NOTICE 'Processing table: bronze.erp_cust_az12';
	start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12
    FROM '/Library/PostgreSQL/17/data/csv_import/cust_az12.csv'
    WITH (FORMAT csv, HEADER, DELIMITER ',');
	end_time := clock_timestamp();
    GET DIAGNOSTICS row_count = ROW_COUNT;
    RAISE NOTICE '  ✓ Loaded % rows into bronze.erp_cust_az12', row_count;
	RAISE NOTICE ' ⏱ Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- Load bronze.erp_loc_a101
    RAISE NOTICE 'Processing table: bronze.erp_loc_a101';
	start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101
    FROM '/Library/PostgreSQL/17/data/csv_import/loc_a101.csv'
    WITH (FORMAT csv, HEADER, DELIMITER ',');
	end_time := clock_timestamp();
    GET DIAGNOSTICS row_count = ROW_COUNT;
    RAISE NOTICE '  ✓ Loaded % rows into bronze.erp_loc_a101', row_count;
	RAISE NOTICE ' ⏱ Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- Load bronze.erp_px_cat_g1v2
    RAISE NOTICE 'Processing table: bronze.erp_px_cat_g1v2';
	start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2
    FROM '/Library/PostgreSQL/17/data/csv_import/px_cat_g1v2.csv'
    WITH (FORMAT csv, HEADER, DELIMITER ',');
	end_time := clock_timestamp();
    GET DIAGNOSTICS row_count = ROW_COUNT;
    RAISE NOTICE '  ✓ Loaded % rows into bronze.erp_px_cat_g1v2', row_count;
	RAISE NOTICE ' ⏱ Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	total_end_time := clock_timestamp();
    RAISE NOTICE '========================================';
    RAISE NOTICE '✅ All bronze tables loaded successfully!';
    RAISE NOTICE '========================================';
	RAISE NOTICE ' ⏱ Total Duration: % seconds', EXTRACT(EPOCH FROM total_end_time - total_start_time);

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '❌ Error occurred during bronze load: %', SQLERRM;
END;
$$;


/*
=======================
Additional notes: 
=======================

First transfer all data files to the server accessible dir
Example command: 
sudo cp 'path/file_name.csv' /path_to_postgres_server/csv_import/

Then the COPY query from /path_to_postgres_server/csv_import/file_name.csv

Use: **SHOW data_directory;** for /path_to_postgres_server/

*/
