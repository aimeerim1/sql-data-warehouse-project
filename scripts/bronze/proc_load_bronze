CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
DECLARE @start_time DATETIME , @end_time DATETIME;
BEGIN TRY
    SEt @start_time=GETDATE();
    TRUNCATE TABLE bronze.crm_cust_info
    BULK INSERT bronze.crm_cust_info
    FROM '/var/opt/mssql/data/cust_info.csv'
    WITH(
        FIRSTROw=2,
        FIELDTERMINATOR=',',
        TABLOCK
    )
     SET @end_time=GETDATE();
    PRINT'>>load DURATION'+cast(DATEDIFF(second,@start_time,@end_time)as NVARCHAR)+'seconds';

    TRUNCATE TABLE bronze.crm_prd_info
    BULK INSERT bronze.crm_prd_info
    FROM '/var/opt/mssql/data/prd_info.csv'
    WITH(
        FIRSTROw=2,
        FIELDTERMINATOR=',',
        TABLOCK
    )

    --  SELECT * from  bronze.crm_prd_info

    TRUNCATE TABLE bronze.crm_sales_details
    BULK INSERT bronze.crm_sales_details
    FROM '/var/opt/mssql/data/sales_details.csv'
    WITH(
        FIRSTROw=2,
        FIELDTERMINATOR=',',
        TABLOCK
    )

    --  SELECT count(*) from  bronze.crm_sales_details
    TRUNCATE TABLE bronze.erp_cust_az12
    BULK INSERT bronze.erp_cust_az12
    FROM '/var/opt/mssql/data/CUST_AZ12.csv'
    WITH(
        FIRSTROw=2,
        FIELDTERMINATOR=',',
        TABLOCK
    )


    TRUNCATE TABLE bronze.erp_loc_a101
    BULK INSERT bronze.erp_loc_a101
    FROM '/var/opt/mssql/data/LOC_A101.csv'
    WITH(
        FIRSTROw=2,
        FIELDTERMINATOR=',',
        TABLOCK
    )

    TRUNCATE TABLE bronze.erp_px_cat_g1v2
    BULK INSERT bronze.erp_px_cat_g1v2
    FROM '/var/opt/mssql/data/PX_CAT_G1V2.csv'
    WITH(
        FIRSTROw=2,
        FIELDTERMINATOR=',',
        TABLOCK
    )
    END TRY

    BEGIN CATCH
            PRINT('=============================================');
            PRINT('ERROR OCCURED DURING LOADING BRONZE LAYER ');
            PRINT('ERROR MESSE:'+ ERROR_MESSAGE());
            PRINT('ERROR NUMBER:'+CAST(ERROR_NUMBER() AS NVARCHAR));
            PRINT('ERROR LINE:'+CAST(ERROR_LINE() AS NVARCHAR));
            PRINT('ERROR PROCEDURE:'+ERROR_PROCEDURE());
            PRINT('=============================================');

    END CATCH
 END
