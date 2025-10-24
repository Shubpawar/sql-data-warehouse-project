IF EXISTS (SELECT 1 FROM sys.databases where name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO



/* the master database is a system database in Sql Server where you can create other databases*/

CREATE DATABASE DataWarehouse;

CREATE SCHEMA bronze;
go  --
CREATE SCHEMA silver;
go --separate batches when working with multiple SQL statements 
CREATE SCHEMA gold;
