-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 2B -  Create Streaming Tables with SQL using Auto Loader
-- MAGIC
-- MAGIC In this demonstration we will create a streaming table to incrementally ingest files from a volume using Auto Loader with SQL. 
-- MAGIC
-- MAGIC When you create a streaming table using the CREATE OR REFRESH STREAMING TABLE statement, the initial data refresh and population begin immediately. These operations do not consume DBSQL warehouse compute. Instead, streaming table rely on serverless DLT for both creation and refresh. A dedicated serverless DLT pipeline is automatically created and managed by the system for each streaming table.
-- MAGIC
-- MAGIC ### Learning Objectives
-- MAGIC
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC - Create streaming tables in Databricks SQL for incremental data ingestion.
-- MAGIC - Refresh streaming tables using the REFRESH statement.
-- MAGIC
-- MAGIC ### RECOMMENDATION
-- MAGIC
-- MAGIC The CREATE STREAMING TABLE SQL command is the recommended alternative to the legacy COPY INTO SQL command for incremental ingestion from cloud object storage. Databricks recommends using streaming tables to ingest data using Databricks SQL. 
-- MAGIC
-- MAGIC A streaming table is a table registered to Unity Catalog with extra support for streaming or incremental data processing. A DLT pipeline is automatically created for each streaming table. You can use streaming tables for incremental data loading from Kafka and cloud object storage.

-- COMMAND ----------

USE CATALOG dbacademy;
USE SCHEMA labuser101;

DROP TABLE IF EXISTS python_csv_autoloader;
DROP TABLE IF EXISTS sql_csv_autoloader;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def create_volume(in_catalog: str, in_schema: str, volume_name: str):
-- MAGIC     '''
-- MAGIC     Create a volume in the specified catalog.schema.
-- MAGIC     '''
-- MAGIC     print(f'Creating volume: {in_catalog}.{in_schema}.{volume_name} if not exists.\n')
-- MAGIC     r = spark.sql(f'CREATE VOLUME IF NOT EXISTS {in_catalog}.{in_schema}.{volume_name}')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f'DROP VOLUME IF EXISTS dbacademy.labuser101.auto_loader_staging_files')
-- MAGIC spark.sql(f'DROP VOLUME IF EXISTS dbacademy.labuser101.csv_files_autoloader_source')
-- MAGIC spark.sql(f'DROP VOLUME IF EXISTS dbacademy.labuser101.auto_loader_files')
-- MAGIC
-- MAGIC create_volume(in_catalog = 'dbacademy', in_schema = f'labuser101', volume_name = 'auto_loader_staging_files')
-- MAGIC create_volume(in_catalog = 'dbacademy', in_schema = f'labuser101', volume_name = 'csv_files_autoloader_source')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def copy_files(copy_from: str, copy_to: str, n: int, sleep=2):
-- MAGIC     '''
-- MAGIC     Copy files from one location to another destination's volume.
-- MAGIC
-- MAGIC     This method performs the following tasks:
-- MAGIC       1. Lists files in the source directory and sorts them. Sorted to keep them in the same order when copying for consistency.
-- MAGIC       2. Verifies that the source directory has at least `n` files.
-- MAGIC       3. Copies files from the source to the destination, skipping files already present at the destination.
-- MAGIC       4. Pauses for `sleep` seconds after copying each file.
-- MAGIC       5. Stops after copying `n` files or if all files are processed.
-- MAGIC       6. Will print information on the files copied.
-- MAGIC     
-- MAGIC     Parameters
-- MAGIC     - copy_from (str): The source directory where files are to be copied from.
-- MAGIC     - copy_to (str): The destination directory where files will be copied to.
-- MAGIC     - n (int): The number of files to copy from the source. If n is larger than total files, an error is returned.
-- MAGIC     - sleep (int, optional): The number of seconds to pause after copying each file. Default is 2 seconds.
-- MAGIC
-- MAGIC     Returns:
-- MAGIC     - None: Prints information to the log on what files it's loading. If the file exists, it skips that file.
-- MAGIC
-- MAGIC     Example:
-- MAGIC     - copy_files(copy_from='/Volumes/gym_data/v01/user-reg', 
-- MAGIC            copy_to=f'{DA.paths.working_dir}/pii/stream_source/user_reg',
-- MAGIC            n=1)
-- MAGIC     '''
-- MAGIC     import os
-- MAGIC     import time
-- MAGIC
-- MAGIC     print(f"\n----------------Loading files to user's volume: '{copy_to}'----------------")
-- MAGIC
-- MAGIC     ## List all files in the copy_from volume and sort the list
-- MAGIC     list_of_files_to_copy = sorted(os.listdir(copy_from))
-- MAGIC     total_files_in_copy_location = len(list_of_files_to_copy)
-- MAGIC
-- MAGIC     ## Get a list of files in the source
-- MAGIC     list_of_files_in_source = os.listdir(copy_to)
-- MAGIC
-- MAGIC     assert total_files_in_copy_location >= n, f"The source location contains only {total_files_in_copy_location} files, but you specified {n}  files to copy. Please specify a number less than or equal to the total number of files available."
-- MAGIC
-- MAGIC     ## Looping counter
-- MAGIC     counter = 1
-- MAGIC
-- MAGIC     ## Load files if not found in the co
-- MAGIC     for file in list_of_files_to_copy:
-- MAGIC       if file.startswith('_'):
-- MAGIC         pass
-- MAGIC       else:
-- MAGIC         ## If the file is found in the source, skip it with a note. Otherwise, copy file.
-- MAGIC         if file in list_of_files_in_source:
-- MAGIC           print(f'File number {counter} - {file} is already in the source volume "{copy_to}". Skipping file.')
-- MAGIC         else:
-- MAGIC           file_to_copy = f'{copy_from}{file}'
-- MAGIC           copy_file_to = f'{copy_to}{file}'
-- MAGIC           print(f'File number {counter} - Copying file {file_to_copy} --> {copy_file_to}.')
-- MAGIC           dbutils.fs.cp(file_to_copy, copy_file_to , recurse = True)
-- MAGIC           
-- MAGIC           ## Sleep after load
-- MAGIC           time.sleep(sleep) 
-- MAGIC
-- MAGIC         ## Stop after n number of loops based on argument.
-- MAGIC         if counter == n:
-- MAGIC           break
-- MAGIC         else:
-- MAGIC           counter = counter + 1

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ## Copy one file to the cloud storage to ingest
-- MAGIC copy_files(copy_from = '/Volumes/dbacademy_ecommerce/v01/raw/sales-csv/', copy_to = f"/Volumes/dbacademy/labuser101/csv_files_autoloader_source/", n=1)
-- MAGIC
-- MAGIC
-- MAGIC ## Copy multiple files to the cloud storage staging area to copy into the above volume during the demonstration
-- MAGIC copy_files(copy_from = '/Volumes/dbacademy_ecommerce/v01/raw/sales-csv/', copy_to = f"/Volumes/dbacademy/labuser101/auto_loader_staging_files/", n=3)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## !!!!! REQUIRED - SELECT YOUR SERVERLESS SQL WAREHOUSE !!!!!
-- MAGIC ## !!!!! REQUIRED - SELECT YOUR SERVERLESS SQL WAREHOUSE !!!!!
-- MAGIC
-- MAGIC **NOTE: Creating streaming tables with Databricks SQL requires a SQL warehouse.**.
-- MAGIC
-- MAGIC ![Select Cluster](./Includes/images/selecting_cluster_info.png)
-- MAGIC
-- MAGIC Before executing cells in this notebook, please select the **SHARED SQL WAREHOUSE** in the lab. Follow these steps:
-- MAGIC
-- MAGIC 1. Navigate to the top-right of this notebook and click the drop-down to select compute (it might say **Connect**). Complete one of the following below:
-- MAGIC
-- MAGIC    a. Under **Recent resources**, check to see if you have a **shared_warehouse SQL**. If you do, select it.
-- MAGIC
-- MAGIC    b. If you do not have a **shared_warehouse** under **Recent resources**, complete the following:
-- MAGIC
-- MAGIC     - In the same drop-down, select **More**.
-- MAGIC
-- MAGIC     - Then select the **SQL Warehouse** button.
-- MAGIC
-- MAGIC     - In the drop-down, make sure **shared_warehouse** is selected.
-- MAGIC
-- MAGIC     - Then, at the bottom of the pop-up, select **Start and attach**.
-- MAGIC
-- MAGIC <br></br>
-- MAGIC    <img src="./Includes/images/sql_warehouse.png" alt="SQL Warehouse" width="600">

-- COMMAND ----------

CREATE OR REPLACE TABLE `dbacademy`.`ops`.`meta` (
  owner STRING,
  object STRING,
  key   STRING,
  value STRING
);


-- COMMAND ----------

INSERT INTO `dbacademy`.`ops`.`meta` VALUES
  ('samira.nigrel@outlook.com', NULL, 'username', 'samira.nigrel@outlook.com'),
  ('samira.nigrel@outlook.com', NULL, 'catalog_name', 'dbacademy'),
  ('samira.nigrel@outlook.com', NULL, 'schema_name', 'ops'),
  ('samira.nigrel@outlook.com', NULL, 'paths.working_dir', '/Volumes/dbacademy/ops/samira.nigrel@outlook.com'),
  ('samira.nigrel@outlook.com', NULL, 'cluster_name', 'samira-cluster'),
  ('samira.nigrel@outlook.com', NULL, 'warehouse_name', 'samira-wh'),
  ('samira.nigrel@outlook.com', NULL, 'pseudonym', 'samira.n'),
  ('account users', NULL, 'datasets.ecommerce', 'dbacademy_ecommerce.v01'),
  ('account users', NULL, 'paths.datasets.ecommerce', '/Volumes/dbacademy_ecommerce/v01');


-- COMMAND ----------

-- Create a temp view storing information from the obs table.
CREATE OR REPLACE TEMP VIEW user_info AS
SELECT map_from_arrays(collect_list(replace(key,'.','_')), collect_list(value))
FROM dbacademy.ops.meta;

-- Create SQL dictionary var (map)
DECLARE OR REPLACE DA MAP<STRING,STRING>;

-- Set the temp view in the DA variable
SET VAR DA = (SELECT * FROM user_info);

DROP VIEW IF EXISTS user_info;

-- COMMAND ----------

-- Change the default catalog/schema
USE CATALOG dbacademy;
USE SCHEMA labuser101;

DROP TABLE IF EXISTS sql_csv_autoloader;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. View the default catalog and schema. Confirm the default catalog is **dbacademy** and the default schema is your **labuser** schema.

-- COMMAND ----------

-- DBTITLE 1,Check your default catalog and schema
SELECT current_catalog(), current_schema()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. Create Streaming Tables for Incremental Processing

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Complete the following to explore the volume `/Volumes/dbacademy/your-lab-user-schema/csv_files_autoloader_source` and confirm it contains a single CSV file.
-- MAGIC
-- MAGIC    a. Select the catalog icon on the left ![Catalog Icon](./Includes/images/catalog_icon.png).
-- MAGIC
-- MAGIC    b. Expand the **dbacademy** catalog.
-- MAGIC
-- MAGIC    c. Expand your **labuser** schema.
-- MAGIC
-- MAGIC    d. Expand **Volumes**.
-- MAGIC
-- MAGIC    e. Expand the **csv_files_autoloader_source** volume.
-- MAGIC
-- MAGIC    f. Confirm it contains a single CSV file named **000.csv**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Run the query below to view the data in the CSV file(s) in your cloud storage location. Notice that it was returned in tabular format and contains 3,149 rows.

-- COMMAND ----------

-- Create the volume
CREATE VOLUME IF NOT EXISTS dbacademy.labuser101.csv_files_autoloader_source
COMMENT 'This is the volume for CSV files';


-- Read the files from the volume
SELECT *
FROM read_files(
  'dbfs:/Volumes/dbacademy/labuser101/csv_files_autoloader_source',
  format => 'CSV',
  sep => '|',
  header => true
);


-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Create a STREAMING TABLE using Databricks SQL
-- MAGIC 3. Your goal is to create an incremental pipeline that only ingests new files (instead of using traditional batch ingestion). You can achieve this by using [streaming tables in Databricks SQL](https://docs.databricks.com/aws/en/dlt/dbsql/streaming) (Auto Loader).
-- MAGIC
-- MAGIC    - The SQL code below creates a streaming table that will be scheduled to incrementally ingest only new data every week. 
-- MAGIC    
-- MAGIC    - A pipeline is automatically created for each streaming table. You can use streaming tables for incremental data loading from Kafka and cloud object storage.
-- MAGIC
-- MAGIC    **NOTE:** Incremental batch ingestion automatically detects new records in the data source and ignores records that have already been ingested. This reduces the amount of data processed, making ingestion jobs faster and more efficient in their use of compute resources.
-- MAGIC
-- MAGIC    **REQUIRED: Please insert the path of your csv_files_autoloader_source volume in the `read_files` function. This process will take about a minute to run and set up the incremental ingestion pipeline.**

-- COMMAND ----------

-- DBTITLE 1,Create a streaming table
-- YOU WILL HAVE TO REPLACE THE EXAMPLE PATH BELOW WITH THE PATH TO YOUR csv_file_autoloader_source VOLUME.
-- You can find the volume in your navigation bar on the right and insert the path
-- OR you can replace `your-labuser-name` with your specific labuser name (name of your schema)

CREATE OR REFRESH STREAMING TABLE sql_csv_autoloader
SCHEDULE EVERY 1 WEEK     -- Scheduling the refresh is optional
AS
SELECT *
FROM STREAM read_files(
  '/Volumes/dbacademy/labuser101/csv_files_autoloader_source',  -- Insert the path to you csv_files_autoloader_source volume (example shown)
  format => 'CSV',
  sep => '|',
  header => true
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Complete the following to view the streaming table in your catalog.
-- MAGIC
-- MAGIC    a. Select the catalog icon on the left ![Catalog Icon](./Includes/images/catalog_icon.png).
-- MAGIC
-- MAGIC    b. Expand the **dbacademy** catalog.
-- MAGIC
-- MAGIC    c. Expand your **labuser** schema.
-- MAGIC
-- MAGIC    d. Expand your **Tables**.
-- MAGIC
-- MAGIC    e. Find the the **sql_csv_autoloader** table. Notice that the Delta streaming table icon is slightly different from a traditional Delta table:
-- MAGIC     
-- MAGIC     ![Streaming table icon](./Includes/images/streaming_table_icon.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. Run the cell below to view the streaming table. Confirm that the results contain **3,149 rows**.

-- COMMAND ----------

-- DBTITLE 1,View the streaming table
SELECT *
FROM sql_csv_autoloader;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6. Describe the STREAMING TABLE and view the results. Notice the following:
-- MAGIC
-- MAGIC - Under **Detailed Table Information**, notice the following rows:
-- MAGIC   - **View Text**: The query that created the table.
-- MAGIC   - **Type**: Specifies that it is a STREAMING TABLE.
-- MAGIC   - **Provider**: Indicates that it is a Delta table.
-- MAGIC
-- MAGIC - Under **Refresh Information**, you can see specific refresh details. Example shown below:
-- MAGIC
-- MAGIC ##### Refresh Information
-- MAGIC
-- MAGIC | Field                   | Value                                                                                                                                         |
-- MAGIC |-------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
-- MAGIC | Last Refreshed          | 2025-06-17T16:12:49.168Z                                                                                                                      |
-- MAGIC | Last Refresh Type       | INCREMENTAL                                                                                                                                   |
-- MAGIC | Latest Refresh Status   | Succeeded                                                                                                                                     |
-- MAGIC | Latest Refresh          | https://example.url.databricks.com/#joblist/pipelines/bed6c715-a7c1-4d45-b57c-4fdac9f956a7/updates/9455a2ef-648c-4339-b61e-d282fa76a92c (this is the path to the Declarative Pipeline that was created for you)|
-- MAGIC | Refresh Schedule        | EVERY 1 WEEKS                                                                                                                                 |

-- COMMAND ----------

-- DBTITLE 1,View the metadata of the streaming table
DESCRIBE TABLE EXTENDED sql_csv_autoloader;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7. The `DESCRIBE HISTORY` statement displays a detailed list of all changes, versions, and metadata associated with a Delta streaming table, including information on updates, deletions, and schema changes.
-- MAGIC
-- MAGIC     Run the cell below and view the results. Notice the following:
-- MAGIC
-- MAGIC     - In the **operation** column, you can see that a streaming table performs three operations: **CREATE TABLE**, **DLT SETUP** and **STREAMING UPDATE**.
-- MAGIC     
-- MAGIC     - Scroll to the right and find the **operationMetrics** column. In row 1 (Version 2 of the table), the value shows that the **numOutputRows** is 3149, indicating that 3149 rows were added to the **sql_csv_autoloader** table.

-- COMMAND ----------

-- DBTITLE 1,View the history of the streaming table
DESCRIBE HISTORY sql_csv_autoloader;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 8. Complete the following steps to manually add another file to your cloud storage location:  
-- MAGIC    `/Volumes/dbacademy/your-lab-user-schema/csv_files_autoloader_source`.
-- MAGIC
-- MAGIC    a. Click the catalog icon on the left ![Catalog Icon](./Includes/images/catalog_icon.png).
-- MAGIC
-- MAGIC    b. Expand the **dbacademy** catalog.
-- MAGIC
-- MAGIC    c. Expand your **labuser** schema.
-- MAGIC
-- MAGIC    d. Expand **Volumes**.
-- MAGIC
-- MAGIC    e. Open the **auto_loader_staging_files** volume.
-- MAGIC
-- MAGIC    f. Right-click on the **001.csv** file and select **Download volume file** to download the file locally.
-- MAGIC
-- MAGIC    g. Upload the downloaded **001.csv** file to the **csv_files_autoloader_source** volume:
-- MAGIC
-- MAGIC       - Right-click on the **csv_files_autoloader_source** volume. 
-- MAGIC
-- MAGIC       - Select **Upload to volume**.  
-- MAGIC
-- MAGIC       - Choose and upload the **001.csv** file from your local machine.
-- MAGIC
-- MAGIC    h. Confirm your volume **csv_files_autoloader_source** contains two CSV files (**000.csv** and **001.csv**).
-- MAGIC
-- MAGIC
-- MAGIC     **NOTE:** Depending on your laptop’s security settings, you may not be able to download files locally.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 9. Next, manually refresh the STREAMING TABLE using `REFRESH STREAMING TABLE table-name`. 
-- MAGIC
-- MAGIC - [Refresh a streaming table](https://docs.databricks.com/aws/en/dlt/dbsql/streaming#refresh-a-streaming-table) documentation
-- MAGIC
-- MAGIC     **NOTE:** You can also go back to **Create a STREAMING TABLE using Databricks SQL (direction number 3)** and rerun that cell to incrementally ingest only new files. Once complete come back to step 8.

-- COMMAND ----------

REFRESH STREAMING TABLE sql_csv_autoloader;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 10. Run the cell below to view the data in the **sql_csv_autoloader** table. Notice that the table now contains **6,081 rows**.
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,View the streaming table
SELECT *
FROM sql_csv_autoloader;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 11. Describe the history of the **sql_csv_autoloader** table. Observe the following:
-- MAGIC
-- MAGIC   - Version 3 of the streaming table includes another **STREAMING UPDATE**.
-- MAGIC
-- MAGIC   - Expand the **operationMetrics** column and note that only **2,932 rows** were incrementally ingested into the table from the new **001.csv** file.
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,View the history of the streaming table
DESCRIBE HISTORY sql_csv_autoloader;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 12. Drop the streaming table.

-- COMMAND ----------

-- DBTITLE 1,Drop the streaming table
DROP TABLE IF EXISTS sql_csv_autoloader;
