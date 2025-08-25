/*
=============================================================================
Quality Checks
=============================================================================
Script Purpose:
  This script performs various quality checks for data consistency, accuracy,
  and standarization across the 'silver' schemas. It includes checks for:
  - Null or duplicate primary keys.
  - Unwanted Spaces instring fields.
  - Data standarization and consistency.
  - Invalid date ranges and orders.
  - Data consistency between related fields.
Usage Notes:
  - Run thse checks after data loading Silver Layer.
  - Investigate and resolve any discrepancies found during the checks.
=============================================================================
*/

-- Cleaning silver.crm_cust_info
-- Check For Nulls or Duplicates in Primary Key
-- Expectation: No Result

SELECT 
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- Check for unwanted Spaces
-- Expectation: No Result
SELECT cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key)

-- Check for unwanted Spaces
-- Expectation: No Result
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

-- Check for unwanted Spaces
-- Expectation: No Result
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

-- Data Standarization & Consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

-- Data Standarization & Consistency
SELECT DISTINCT cst_material_status
FROM silver.crm_cust_info

SELECT * FROM silver.crm_cust_info

SELECT DISTINCT cst_gndr FROM bronze.crm_cust_info


-- =======================================
-- Cleaning silver.crm_prd_info
SELECT
	prd_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info

-- Check For Nulls or Duplicates in Primary Key
-- Expectation: No Result

SELECT 
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- Check for unwanted Spaces
-- Expectation: No Result
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for nulls or negative numbers
-- Expectation: No Result
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Data standarization & consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- Check for invalid date orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_END_dt < prd_start_dt

SELECT *
FROM silver.crm_prd_info


-- =======================================
-- Cleaning silver.crm_sales_details
-- Check for invalid dates
SELECT 
NULLIF (sls_order_dt,0) sls_order_dt 
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0


-- Check data consistency: between sales, quantity and price
-- Sales = Quantity * Price
-- values must not be NULL, zero or negative.

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales, sls_quantity, sls_price

-- Check for invalid date orders
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

SELECT *
FROM silver.crm_sales_details

-- =======================================
-- Cleaning silver.erp_cust_az12
-- identify out of range dates
SELECT DISTINCT
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- data standarization & consistency
SELECT DISTINCT gen
FROM silver.erp_cust_az12

-- data standarization & consistency
SELECT DISTINCT 
gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12

SELECT *
FROM silver.erp_cust_az12

-- =======================================
-- Cleaning silver.erp_loc_a101

SELECT *
FROM bronze.erp_loc_a101;

-- data standarization & consistency
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry

SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry

SELECT DISTINCT 
cntry AS old_country,
CASE WHEN TRIM (cntry) = 'DE' THEN 'Germany'
	 WHEN TRIM (cntry) IN ('US', 'USA') THEN 'United States'
	 WHEN TRIM (cntry) = '' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM (cntry)
END AS cntry
FROM bronze.erp_loc_a101
ORDER BY cntry

SELECT *
FROM silver.erp_loc_a101;

-- =======================================
-- Cleaning silver.erp_px_cat_g1v2

SELECT DISTINCT prd_key
FROM silver.crm_prd_info
ORDER BY prd_key

SELECT DISTINCT id
FROM bronze.erp_px_cat_g1v2
ORDER BY id

-- Check for unwanted Spaces
-- Expectation: No Result
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

-- data standarization & consistency
SELECT
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2

SELECT *
FROM silver.erp_px_cat_g1v2;


EXEC bronze.load_bronze;

EXEC silver.load_silver;
