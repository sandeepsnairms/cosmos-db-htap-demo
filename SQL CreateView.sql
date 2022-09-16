--Create Credentials
-- use SQL Account PRIMARY Key as SECRET
CREATE CREDENTIAL MyCosmosDBAccountCredential
WITH IDENTITY = 'SHARED ACCESS SIGNATURE', SECRET = '{Cosmos PRIMARY Key}';
GO

--Raw Query
SELECT *
       FROM OPENROWSET(
              PROVIDER = 'CosmosDB',
			  CONNECTION = 'Account={Cosmos Account Name};Database=ContosoMobile',
			  OBJECT = 'CallRecords',
			  SERVER_CREDENTIAL = 'MyCosmosDBAccountCredential'
	  )
         as [CallRecords]


-- Query WITH Clause
-- The clause is used for defining a temporary relation such that the output of this temporary relation is available and
-- is used by the query that is associated with the WITH clause
SELECT *
       FROM OPENROWSET(
              PROVIDER = 'CosmosDB',
			  CONNECTION = 'Account={Cosmos Account Name};Database=ContosoMobile',
			  OBJECT = 'CallRecords',
			  SERVER_CREDENTIAL = 'MyCosmosDBAccountCredential'
	  )
        -- as [CallRecords]
       WITH (
              StartDateTime_STR VARCHAR(256) '$.StartDateTime.string' ,
              EndDateTime_STR VARCHAR(256) '$.EndDateTime.string' ,
              DurationSec INT '$.DurationSec.num',
              CallFrom VARCHAR(256) COLLATE Latin1_General_100_BIN2_UTF8 '$.CallFrom.string',
              CallTo VARCHAR(256) COLLATE Latin1_General_100_BIN2_UTF8 '$.CallTo.string',
              CallType VARCHAR(256) COLLATE Latin1_General_100_BIN2_UTF8 '$.CallType.string',
              CallLocationId INT '$.CallLocationId.num',
              BaseLocationId INT '$.BaseLocationId.num',
              IsRoaming BIT '$.IsRoaming.bool',
              IsIncoming BIT '$.IsRoaming.bool',       
              SubscriberId VARCHAR(256) COLLATE Latin1_General_100_BIN2_UTF8 '$.SubscriberId.string',
              BillCycle VARCHAR(256) COLLATE Latin1_General_100_BIN2_UTF8 '$.BillCycle.string',
              DeviceModel VARCHAR(256) COLLATE Latin1_General_100_BIN2_UTF8 '$.device.object.Make.string',
              DeviceOS VARCHAR(256) COLLATE Latin1_General_100_BIN2_UTF8 '$.device.object.OS.string',
              pk VARCHAR(256) COLLATE Latin1_General_100_BIN2_UTF8 '$.pk.string'                                   
       )
       as [CallRecords]       
       where  CallRecords.BillCycle='JUN2022'


--Create Database to save views.
CREATE DATABASE contosomobilelinksql COLLATE Latin1_General_100_BIN2_UTF8
GO

-- Switch to the newly created database
-- Create View
CREATE VIEW CallRecordsView_SQL
AS SELECT  StartDateTime_STR,EndDateTime_STR,DurationSec,CallFrom,CallTo,CallType, CallLocationId,BaseLocationId,IsRoaming,IsIncoming,SubscriberId,BillCycle,DeviceModel,DeviceOS
       FROM OPENROWSET(
              PROVIDER = 'CosmosDB',
			  CONNECTION = 'Account={Cosmos Account Name};Database=ContosoMobile',
			  OBJECT = 'CallRecords',
			  SERVER_CREDENTIAL = 'MyCosmosDBAccountCredential'
	  )
       WITH (
              StartDateTime_STR VARCHAR(256) '$.StartDateTime.string' ,
              EndDateTime_STR VARCHAR(256) '$.EndDateTime.string' ,
              DurationSec INT '$.DurationSec.num',
              CallFrom VARCHAR(256) COLLATE Latin1_General_100_BIN2_UTF8 '$.CallFrom.string',
              CallTo VARCHAR(256) COLLATE Latin1_General_100_BIN2_UTF8 '$.CallTo.string',
              CallType VARCHAR(256) COLLATE Latin1_General_100_BIN2_UTF8 '$.CallType.string',
              CallLocationId INT '$.CallLocationId.num',
              BaseLocationId INT '$.BaseLocationId.num',
              IsRoaming BIT '$.IsRoaming.bool',
              IsIncoming BIT '$.IsRoaming.bool',       
              SubscriberId VARCHAR(256) COLLATE Latin1_General_100_BIN2_UTF8 '$.SubscriberId.string',
              BillCycle VARCHAR(256) COLLATE Latin1_General_100_BIN2_UTF8 '$.BillCycle.string',
              DeviceModel VARCHAR(256) COLLATE Latin1_General_100_BIN2_UTF8 '$.device.object.Make.string',
              DeviceOS VARCHAR(256) COLLATE Latin1_General_100_BIN2_UTF8 '$.device.object.OS.string',
              pk VARCHAR(256) COLLATE Latin1_General_100_BIN2_UTF8 '$.pk.string'     
       )as [CallRecords]

-- Execute the view from any third party application like PowerBi or SQL Server Management Studio