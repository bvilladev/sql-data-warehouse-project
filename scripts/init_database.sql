/*
=============================================================
Database Initialization Script - DataWarehouse
=============================================================

Author:        Brayan Alexander Villanueva Quispe
Created:       2026-03-26  
Environment:   SQL Server  

-------------------------------------------------------------
Description:
    This script initializes the 'DataWarehouse' database.
    It performs the following actions:

    1. Checks if the database already exists
    2. Drops the database safely (if it exists)
    3. Creates a new database
    4. Creates schemas:
        - bronze (raw data)
        - silver (cleaned data)
        - gold (business-ready data)

-------------------------------------------------------------
WARNING:
    This script will permanently DELETE the existing 
    'DataWarehouse' database (if it exists).

    All data will be lost. Use with caution.

-------------------------------------------------------------
Best Practices:
    - Do not run in production without approval
    - Ensure backups exist before execution
=============================================================
*/

USE master;
GO

-------------------------------------------------------------
-- Drop database if exists
-------------------------------------------------------------
IF EXISTS (
    SELECT 1
    FROM sys.databases
    WHERE name = 'DataWarehouse'
)
BEGIN
    PRINT 'Dropping existing database: DataWarehouse';

    ALTER DATABASE DataWarehouse 
    SET SINGLE_USER 
    WITH ROLLBACK IMMEDIATE;

    DROP DATABASE DataWarehouse;
END;
GO

-------------------------------------------------------------
-- Create database
-------------------------------------------------------------
PRINT 'Creating database: DataWarehouse';

CREATE DATABASE DataWarehouse;
GO

-------------------------------------------------------------
-- Use database
-------------------------------------------------------------
USE DataWarehouse;
GO

-------------------------------------------------------------
-- Create schemas
-------------------------------------------------------------
PRINT 'Creating schema: bronze';
CREATE SCHEMA bronze;
GO

PRINT 'Creating schema: silver';
CREATE SCHEMA silver;
GO

PRINT 'Creating schema: gold';
CREATE SCHEMA gold;
GO

-------------------------------------------------------------
-- End of script
-------------------------------------------------------------
PRINT 'Database and schemas created successfully';
GO
