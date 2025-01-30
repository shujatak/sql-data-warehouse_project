/*
  Stored Procedure: Load Bronze Layer (Source -> Bronze)
  ======================================================
  Script purpose:
    To load data into the bronze schema from external CSV files.
    To perform the following actions:
      - Trucate teh bronze tables before loading data
      - Use the BULK INSERT methhod to load data from CSV files to bronze talbes
  Parameters: None
    This stored procedure does not accpet any parameters or returned values.
  Usage Example: EXED bronze.load_bronze
*/

--USE DataWareHouse
	-- EXEC bronze.load_bronze

	CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
	BEGIN
		--Variables to track ETL Duration
		DECLARE @start_time DATETIME, @end_time DATETIME, @start_loading_layer DATETIME, @end_loading_layer DATETIME;
		-- Error handling
		BEGIN TRY
		SET @start_loading_layer = GETDATE();
			-- Bulk insert CRM data
			PRINT '===============================================';
			PRINT 'Loading the bronze layer';	
			PRINT '===============================================';
			-- Table: bronze.crm_cust_info
			PRINT '===============================================';
			PRINT 'Loading the CRM tables';	
			PRINT '===============================================';

			SET @start_time = GETDATE();
			
			PRINT '>>> Truncating the table bronze.crm_cust_info'; 
			TRUNCATE TABLE bronze.crm_cust_info;

			PRINT '>>> Inserting data into bronze.crm_cust_info'; 
			BULK INSERT bronze.crm_cust_info
			FROM 'D:\Data Analytics\Data Warehousing\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT('===============================================');

			--SELECT * FROM bronze.crm_cust_info;
			--SELECT COUNT(*) FROM bronze.crm_cust_info;

			-- Table: bronze.crm_prd_info
			SET @start_time = GETDATE();
			PRINT '>>> Truncating the table bronze.crm_prd_info';
			TRUNCATE TABLE bronze.crm_prd_info;

			PRINT '>>> Inserting data into bronze.crm_prd_info';
			BULK INSERT bronze.crm_prd_info
			FROM 'D:\Data Analytics\Data Warehousing\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);

			SET @end_time = GETDATE();
			PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT('===============================================');
			--SELECT COUNT(*) FROM bronze.crm_prd_info;
			--SELECT * FROM bronze.crm_prd_info;

			-- Table: bronze.crm_sales_details
			SET @start_time = GETDATE();
			PRINT '>>> Truncating the table bronze.crm_sales_details';
			TRUNCATE TABLE bronze.crm_sales_details;
	
			PRINT '>>> Inserting data into bronze.crm_sales_details';
			BULK INSERT bronze.crm_sales_details
			FROM 'D:\Data Analytics\Data Warehousing\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT('===============================================');

			-- Termporaritly done to verify the inserted data --
			--SELECT COUNT(*) FROM bronze.crm_sales_details;
			--SELECT * FROM bronze.crm_sales_details;


			-- Bulk insert ERP data
			-- Table: bronze.erp_cust_az12

			PRINT '===============================================';
			PRINT 'Loading the ERP tables';	
			PRINT '===============================================';

			-- Table: bronze.erp_cust_az12
			SET @start_time = GETDATE();
			PRINT '>>> Truncating the table bronze.erp_cust_az12';
			TRUNCATE TABLE bronze.erp_cust_az12;

			PRINT '>>> Inserting data into bronze.erp_cust_az12';
			BULK INSERT bronze.erp_cust_az12
			FROM 'D:\Data Analytics\Data Warehousing\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT('===============================================');
			--SELECT COUNT(*) FROM bronze.erp_cust_az12;
			--SELECT * FROM bronze.erp_cust_az12;

			-- Table: bronze.erp_loc_a101
			SET @start_time = GETDATE();
			PRINT '>>> Truncating the table bronze.erp_loc_a101';
			TRUNCATE TABLE bronze.erp_loc_a101;

			PRINT '>>> Inserting data into bronze.erp_loc_a101';
			BULK INSERT bronze.erp_loc_a101
			FROM 'D:\Data Analytics\Data Warehousing\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT('===============================================');
			--SELECT COUNT(*) FROM bronze.erp_loc_a101;
			--SELECT * FROM bronze.erp_loc_a101;

			-- Table: bronze.erp_px_cat_g1v2
			SET @start_time = GETDATE();
			PRINT '>>> Truncating the table bronze.erp_px_cat_g1v2';
			TRUNCATE TABLE bronze.erp_px_cat_g1v2;

			PRINT '>>> Inserting data into bronze.erp_px_cat_g1v2';
			BULK INSERT bronze.erp_px_cat_g1v2
			FROM 'D:\Data Analytics\Data Warehousing\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT('===============================================');
			--SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2;
			--SELECT * FROM bronze.erp_px_cat_g1v2;

			SET @end_loading_layer = GETDATE();
			PRINT 'Bronze Layer Loadeed in: ' + CAST(DATEDIFF(second, @start_loading_layer, @end_loading_layer) AS NVARCHAR) + ' seconds';
		END TRY

		BEGIN CATCH
			PRINT '===============================================';
			PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
			PRINT 'Error Message: ' + ERROR_MESSAGE();
			PRINT 'Error Number: ' + CAST (ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error State: ' + CAST (ERROR_STATE() AS NVARCHAR);
			PRINT '===============================================';
		END CATCH
	END
