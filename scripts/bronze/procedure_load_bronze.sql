/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Author:        Brayan Alexander Villanueva Quispe
Created:       2026-04-05  
Environment:   SQL Server  

-------------------------------------------------------------------------------
Description:
    This stored procedure loads data into the Bronze layer of the Data Warehouse
    from external CSV files located in the local file system.

    The process follows a full refresh strategy (TRUNCATE + LOAD) and is designed
    to populate raw data tables without transformations.

-------------------------------------------------------------------------------
Process Overview:
    The procedure executes the following steps:

    1. Initializes batch execution time tracking.
    2. Loads CRM source data:
        - bronze.crm_cust_info
        - bronze.crm_prd_info
        - bronze.crm_sales_details
    3. Loads ERP source data:
        - bronze.erp_cust_az12
        - bronze.erp_loc_a101
        - bronze.erp_px_cat_g1v2
    4. For each table:
        - Truncates existing data
        - Loads new data using BULK INSERT from CSV files
        - Measures and prints execution time
    5. Displays total execution time of the batch
    6. Handles errors using TRY...CATCH block

-------------------------------------------------------------------------------
Data Sources:
    CRM:
        - cust_info.csv
        - prd_info.csv
        - sales_details.csv

    ERP:
        - CUST_AZ12.csv
        - LOC_A101.csv
        - PX_CAT_G1V2.csv

    File Location:
        D:\datos\DataWarehouse\datasets\

-------------------------------------------------------------------------------
WARNING:
    - This procedure uses TRUNCATE TABLE, which permanently deletes all existing
      data in the target tables before loading new data.
    - Ensure source files are available and accessible before execution.
    - Data loss will occur if executed without proper backup.

-------------------------------------------------------------------------------
Best Practices:
    - Execute in controlled environments (Development / Staging)
    - Validate file paths and permissions before running
    - Monitor execution logs and duration
    - Avoid running during peak database usage
    - Consider incremental loads for large datasets in production

-------------------------------------------------------------------------------
Error Handling:
    - Captures and prints:
        * Error message
        * Error number
        * Error state
    - Stops execution if any failure occurs during loading

-------------------------------------------------------------------------------
Parameters:
    None.
    This stored procedure does not accept parameters.

-------------------------------------------------------------------------------
Usage Example:
    EXEC bronze.load_bronze;

===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze 
AS
BEGIN
	DECLARE 
		@start_time DATETIME, 
		@end_time DATETIME, 
		@batch_start_time DATETIME, 
		@batch_end_time DATETIME;

	BEGIN TRY
		SET @batch_start_time = GETDATE();

		PRINT '==========================================';
		PRINT('Loading bronze layeer');
		PRINT '==========================================';

		PRINT('------------------------------------------');
		PRINT('Loading CRM table');
		PRINT('------------------------------------------');

		--======================================================
		SET @start_time = GETDATE();

		PRINT('>> Truncate table: bronze.crm_cust_info');
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT('>> Insert data Into: bronze.crm_cust_info');
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\datos\DataWarehouse\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT('Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');
		PRINT('------------------------------------------');

		--======================================================
		SET @start_time = GETDATE();

		PRINT('>> Truncate table: bronze.crm_prd_info');
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT('>> Insert data Into: bronze.crm_prd_info');
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\datos\DataWarehouse\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT('Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');
		PRINT('------------------------------------------');

		--======================================================
		SET @start_time = GETDATE();

		PRINT('>> Truncate table: bronze.crm_sales_details');
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT('>> Insert data Into: bronze.crm_sales_details');
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\datos\DataWarehouse\datasets\source_crm\sales_details.CSV'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT('Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');
		PRINT('------------------------------------------');

		--======================================================
		SET @start_time = GETDATE();

		PRINT('>> Truncate table: bronze.erp_cust_az12');
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT('>> Insert data Into: bronze.erp_cust_az12');
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\datos\DataWarehouse\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT('Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');
		PRINT('------------------------------------------');

		--======================================================
		SET @start_time = GETDATE();

		PRINT('>> Truncate table: bronze.erp_loc_a101');
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT('>> Insert data Into: bronze.erp_loc_a101');
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\datos\DataWarehouse\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT('Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');
		PRINT('------------------------------------------');

		--======================================================
		SET @start_time = GETDATE();

		PRINT('>> Truncate table: bronze.erp_px_cat_g1v2');
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT('>> Insert data Into: bronze.erp_px_cat_g1v2');
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\datos\DataWarehouse\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT('Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');
		PRINT('------------------------------------------');

		--======================================================
		SET @end_time = GETDATE();
		PRINT '==========================================';
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '==========================================';

	END TRY 
	BEGIN CATCH

		PRINT '==========================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Message: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==========================================';

	END CATCH
END;

