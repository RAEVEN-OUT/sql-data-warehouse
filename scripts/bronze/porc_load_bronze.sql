-- Created by GitHub Copilot in SSMS - review carefully before executing
/*
Loads source data from CSV files into all bronze layer tables.
This procedure truncates and reloads CRM and ERP data from their respective source files.
Includes per-table duration tracking and comprehensive error handling with timing.
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
	DECLARE @StartTime DATETIME2 = SYSDATETIME();
	DECLARE @EndTime DATETIME2;
	DECLARE @DurationSeconds DECIMAL(10, 2);
	DECLARE @TableStartTime DATETIME2;
	DECLARE @TableEndTime DATETIME2;
	DECLARE @TableDurationSeconds DECIMAL(10, 2);

	BEGIN TRY
		PRINT '';
		PRINT '========================================';
		PRINT 'STARTING BRONZE LAYER DATA LOAD';
		PRINT 'Start Time: ' + CONVERT(VARCHAR(30), @StartTime, 121);
		PRINT '========================================';
		PRINT '';

		-- ===== LOADING CRM TABLES =====
		PRINT '>>> LOADING CRM TABLES';
		PRINT '';

		-- CRM Customer Info
		PRINT '  [1/6] CRM Customer Info (crm_cust_info)';
		PRINT '    - Truncating table...';
		SET @TableStartTime = SYSDATETIME();
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '    - Inserting data from CSV...';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\ravee\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @TableEndTime = SYSDATETIME();
		SET @TableDurationSeconds = CAST(DATEDIFF(MILLISECOND, @TableStartTime, @TableEndTime) AS DECIMAL(10, 2)) / 1000.0;
		PRINT '    - Complete! Duration: ' + FORMAT(@TableDurationSeconds, '0.00') + ' seconds';
		PRINT '';

		-- CRM Product Info
		PRINT '  [2/6] CRM Product Info (crm_prd_info)';
		PRINT '    - Truncating table...';
		SET @TableStartTime = SYSDATETIME();
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '    - Inserting data from CSV...';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\ravee\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @TableEndTime = SYSDATETIME();
		SET @TableDurationSeconds = CAST(DATEDIFF(MILLISECOND, @TableStartTime, @TableEndTime) AS DECIMAL(10, 2)) / 1000.0;
		PRINT '    - Complete! Duration: ' + FORMAT(@TableDurationSeconds, '0.00') + ' seconds';
		PRINT '';

		-- CRM Sales Details
		PRINT '  [3/6] CRM Sales Details (crm_sales_details)';
		PRINT '    - Truncating table...';
		SET @TableStartTime = SYSDATETIME();
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '    - Inserting data from CSV...';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\ravee\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @TableEndTime = SYSDATETIME();
		SET @TableDurationSeconds = CAST(DATEDIFF(MILLISECOND, @TableStartTime, @TableEndTime) AS DECIMAL(10, 2)) / 1000.0;
		PRINT '    - Complete! Duration: ' + FORMAT(@TableDurationSeconds, '0.00') + ' seconds';
		PRINT '';

		-- ===== LOADING ERP TABLES =====
		PRINT '>>> LOADING ERP TABLES';
		PRINT '';

		-- ERP Customer
		PRINT '  [4/6] ERP Customer (erp_cust_az12)';
		PRINT '    - Truncating table...';
		SET @TableStartTime = SYSDATETIME();
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '    - Inserting data from CSV...';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\ravee\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @TableEndTime = SYSDATETIME();
		SET @TableDurationSeconds = CAST(DATEDIFF(MILLISECOND, @TableStartTime, @TableEndTime) AS DECIMAL(10, 2)) / 1000.0;
		PRINT '    - Complete! Duration: ' + FORMAT(@TableDurationSeconds, '0.00') + ' seconds';
		PRINT '';

		-- ERP Location
		PRINT '  [5/6] ERP Location (erp_loc_a101)';
		PRINT '    - Truncating table...';
		SET @TableStartTime = SYSDATETIME();
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '    - Inserting data from CSV...';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\ravee\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @TableEndTime = SYSDATETIME();
		SET @TableDurationSeconds = CAST(DATEDIFF(MILLISECOND, @TableStartTime, @TableEndTime) AS DECIMAL(10, 2)) / 1000.0;
		PRINT '    - Complete! Duration: ' + FORMAT(@TableDurationSeconds, '0.00') + ' seconds';
		PRINT '';

		-- ERP Product Category
		PRINT '  [6/6] ERP Product Category (erp_px_cat_g1v2)';
		PRINT '    - Truncating table...';
		SET @TableStartTime = SYSDATETIME();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '    - Inserting data from CSV...';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\ravee\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @TableEndTime = SYSDATETIME();
		SET @TableDurationSeconds = CAST(DATEDIFF(MILLISECOND, @TableStartTime, @TableEndTime) AS DECIMAL(10, 2)) / 1000.0;
		PRINT '    - Complete! Duration: ' + FORMAT(@TableDurationSeconds, '0.00') + ' seconds';
		PRINT '';

		-- ===== COMPLETION =====
		SET @EndTime = SYSDATETIME();
		SET @DurationSeconds = CAST(DATEDIFF(MILLISECOND, @StartTime, @EndTime) AS DECIMAL(10, 2)) / 1000.0;

		PRINT '========================================';
		PRINT 'BRONZE LAYER DATA LOAD COMPLETED';
		PRINT 'End Time: ' + CONVERT(VARCHAR(30), @EndTime, 121);
		PRINT 'Total ETL Duration: ' + FORMAT(@DurationSeconds, '0.00') + ' seconds';
		PRINT '========================================';
		PRINT '';
	END TRY
	BEGIN CATCH
		PRINT '';
		PRINT '========================================';
		PRINT 'ERROR OCCURRED DURING BRONZE LAYER LOAD';
		PRINT '========================================';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
		PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR(10));
		PRINT '';
		SET @EndTime = SYSDATETIME();
		SET @DurationSeconds = CAST(DATEDIFF(MILLISECOND, @StartTime, @EndTime) AS DECIMAL(10, 2)) / 1000.0;
		PRINT 'End Time: ' + CONVERT(VARCHAR(30), @EndTime, 121);
		PRINT 'Execution Duration Before Failure: ' + FORMAT(@DurationSeconds, '0.00') + ' seconds';
		PRINT '========================================';
		PRINT '';
		
		-- Re-throw the error so the caller knows the procedure failed
		THROW;
	END CATCH
END;

-- EXEC bronze.load_bronze;
