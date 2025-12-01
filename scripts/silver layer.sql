--=========================================================

--STORED PROCEDURE FOR SILVER LAYER---

--=========================================================

--Check for nulls or duplicates in primary key
--Expectation: No result

/*
SELECT 
cst_int,  --cst_id = cst_int
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_int
HAVING COUNT(*) > 1 OR cst_int IS NULL
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN 
	DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT'============================================';
		PRINT 'Loading Bronze Layer' ;
		PRINT'============================================';
		PRINT'----------------------------------------';
		PRINT'Loading CRM Tables';
		PRINT'----------------------------------------';
		SET @start_time = GETDATE();
	PRINT 'Truncatin Table: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT 'Inserting Data Into: silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info(cst_int,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_material_status,
		cst_gendr,
		cst_create_date)
	SELECT 
	cst_int,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
		 WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
		 ELSE 'n/a'
	END cst_material_status,
	CASE WHEN UPPER(TRIM(cst_gendr)) = 'F' THEN 'Female'
		 WHEN UPPER(TRIM(cst_gendr)) = 'M' THEN 'Male'
		 ELSE 'n/a'
	END cst_gendr,
	cst_create_date
	FROM (
	SELECT 
	*,
	ROW_NUMBER() OVER (PARTITION BY  cst_int ORDER BY cst_create_date DESC) AS flag_last  --Assign a unique number to each row in a result set, based on a defined order
	FROM bronze.crm_cust_info
	)t WHERE flag_last = 1
			SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ 'seconds';
		PRINT '>> -----------------------------';

	/*	DATA QUALITY CHECKS
	--Check for unwanted Spaces
	--Expectation : NO Results
	/*
	SELECT cst_firstname
	FROM bronze.crm_cust_info
	WHERE cst_firstname != TRIM(cst_firstname)
	*/

	--SELECT TOP 3 * FROM bronze.crm_cust_info;

	--DATA Standatdization & consistency
	SELECT DISTINCT cst_gendr
	FROM bronze.crm_cust_info;


	--Check the silver layer fdata quality
	--Expectation : NO Results
	SELECT cst_firstname
	FROM silver.crm_cust_info
	WHERE cst_firstname != TRIM(cst_firstname)


	--DATA Standatdization & consistency
	SELECT DISTINCT cst_gendr
	FROM silver.crm_cust_info;

	--Check for nulls or duplicates in primary key
	--Expectation: No result

	SELECT 
	cst_int,  --cst_id = cst_int
	COUNT(*)
	FROM silver.crm_cust_info
	GROUP BY cst_int
	HAVING COUNT(*) > 1 OR cst_int IS NULL

	SELECT * FROM silver.crm_cust_info
	*/


	--product info 
	PRINT '>> Truncatin Table: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT '>> Inserting Data Into: silver.crm_prd_info'
	INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt)	
	SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key,1, 5),'-','_') AS cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN 'other Sales'
			 WHEN 'T' THEN 'Touring'
			 ELSE 'n/a'
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
	FROM bronze.crm_prd_info
	--WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) IN(  --here there are just products which dont have any orders which is fine
	--SELECT sls_prd_key FROM bronze.crm_sales_details);
	--WHERE REPLACE(SUBSTRING(prd_key,1, 5),'-','_') NOT IN -- TO FILTER OUT UNMATCHED DATA AFTER APPLYING TRANSFORMATION
	--(SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2);


	--Check for NUll or duplicates in Primary Key
	--Expectation: No Result
	/*
	SELECT
	prd_id,
	COUNT(*)
	FROM bronze.crm_prd_info
	GROUP BY prd_id
	HAVING COUNT(*) > 1 OR prd_id IS NULL
	*/

	--SELECT DISTINCT id FROM silver.erp_px_cat_g1v2;
	--we have to join this with product_info table but there is 
	--one catch that here ther is '_' as second character instead of '-' eg. erp=CO_RF crm=CO-RF
	--so we need to change that 

	--to see the prd_key details in table
	--SELECT sls_prd_key FROM silver.crm_sales_details;


	--Check for Invalid Date Orders
	/*SELECT * 
	FROM silver.crm_prd_info
	WHERE prd_end_dt < prd_start_dt
	*/

	--Sales details
	PRINT '>> Truncatin Table: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	PRINT '>> Inserting Data Into: silver.crm_sales_details';
	INSERT INTO silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	)
	SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR lEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR lEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
	--Check for invalid Dates
	/*SELECT 
	NULLIF(sls_ship_dt,0) sls_ship_dt
	FROM bronze.crm_sales_details
	WHERE sls_ship_dt <= 0 
	OR LEN(sls_ship_dt)!= 8
	OR sls_ship_dt > 20500101
	OR sls_ship_dt < 19000101
	since the "sls_ship_dt" show no result here it means it can be used as 
	it is from the bronze layer. But as a precaution we will apply the same preprossing.
	*/
		CASE WHEN sls_due_dt = 0 OR lEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		sls_quantity,
	--for bad data in 
	--sales: if negative, zero, or null, derive it using Quantity and Price
	--price: if zero or null, calculate using Sales and Quantity
	--if price is negative, convert it to a positive value
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,
	CASE WHEN sls_price IS NULL OR sls_price <= 0 
			THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
	END AS sls_price
	FROM bronze.crm_sales_details
	--WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)


	--Check for invalid date orders
	/*SELECT 
	*
	FROM silver.crm_sales_details
	WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt
	SELECT * FROM silver.crm_sales_details
	*/


	--Customer Details
	PRINT '>> Truncatin Table: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT '>> Inserting Data Into: silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
	SELECT
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
		 ELSE cid
	END AS cid,
	CASE WHEN bdate > GETDATE() THEN NULL
		 ELSE bdate
	END AS bdate,
	CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female' 
		 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male' 
		 ELSE 'n/a'
	END AS gen
	FROM bronze.erp_cust_az12
	/*WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
		 ELSE cid
	END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)
	--i.e. if result as empty column then we will not be able to find any unmatching
	--data between the customer info from erp and crm systems
	*/


	--SELECT * FROM silver.crm_cust_info

	/*
	--Data Standardization & consistency
	SELECT DISTINCT 
	gen,
	CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female' --always make sure the 'IN' should have capital letters if using UPPER() Function
		 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male' 
		 ELSE 'n/a'
	END AS gen
	FROM bronze.erp_cust_az12
	*/

	--Location info
	PRINT '>> Truncatin Table: silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT '>> Inserting Data Into: silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101
	(cid,cntry)
	SELECT 
	REPLACE(cid,'-',''),
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN('US','USA') THEN 'United States'
		 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		 ELSE TRIM(cntry)
	END AS cntry
	FROM bronze.erp_loc_a101;

	--SELECT cst_key FROM silver.crm_cust_info;


	--Product Category information
	PRINT '>> Truncatin Table: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2
	(id,cat,subcat,maintenance)
	SELECT
	id,
	cat,
	subcat,
	maintenance
	FROM bronze.erp_px_cat_g1v2
END


EXEC silver.load_silver
