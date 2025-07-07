CREATE SCHEMA eucautomation.aks_dataset;

-- Input_Data_1 – Customer Transaction
CREATE TABLE `eucautomation.aks_dataset.Input_Data_1` (
    Customer_ID STRING(10),
    Product_ID STRING(20),
    Quantity INT,
    Transaction_Date DATE
);

-- Input_Data_2 – Product Info
CREATE TABLE `eucautomation.aks_dataset.Input_Data_2` (
    Product_ID STRING(20),
    Category STRING(10),
    Product_Name STRING(50),
    Price DECIMAL(10, 2)
);

-- Ref_Data – Category to Region Mapping
CREATE TABLE `eucautomation.aks_dataset.Ref_Data` (
    Category STRING(10),
    Region STRING(20),
    Department STRING(30)
);

CREATE TABLE `eucautomation.aks_dataset.config_table` (
  step_id INT64 NOT NULL,
  stage STRING,
  description STRING,
  sql STRING
);


CREATE TABLE `eucautomation.aks_dataset.config_rules` (
  step_id INT64,
  rule_id STRING,
  description STRING,
  comment STRING,
  statement STRING
);

CREATE TABLE `eucautomation.aks_dataset.exception_report` (
  execution_id STRING,
  step_id INT64,
  rule_id STRING,
  rule_description STRING,
  comment STRING,
  failed_row JSON,
  timestamp TIMESTAMP
);

CREATE TABLE `eucautomation.aks_dataset.pipeline_step_logs` (
  execution_id STRING,
  step_id INT64,
  stage STRING,
  description STRING,
  status STRING,
  timestamp TIMESTAMP,
  error_message STRING
);


INSERT INTO `eucautomation.aks_dataset.config_rules` (step_id, rule_id, description, comment, statement)
VALUES
  (1, "R1_01", "Negative Quantity", "Quantity must be ≥ 0", "SELECT TO_JSON_STRING(t) FROM `eucautomation.aks_dataset.Input_Data_1` t WHERE Quantity < 0"),
  (1, "R1_02", "Missing Product ID", "Product ID should not be NULL", "SELECT TO_JSON_STRING(t) FROM `eucautomation.aks_dataset.Input_Data_1` t WHERE Product_ID IS NULL"),
  (2, "R2_01", "Missing Category", "Product enrichment failed", "SELECT TO_JSON_STRING(t) FROM `eucautomation.aks_dataset.stage2` t WHERE Category IS NULL"),
  (3, "R3_01", "Zero Total Cost", "Total Cost must be > 0", "SELECT TO_JSON_STRING(t) FROM `eucautomation.aks_dataset.stage3` t WHERE Total_Cost <= 0");



INSERT INTO `eucautomation.aks_dataset.Input_Data_1` (Customer_ID, Product_ID, Quantity, Transaction_Date)
VALUES 
  ('C001', 'P100_1', 2, DATE '2025-07-01'),
  ('C002', 'P101_1', 1, DATE '2025-07-02'),
  ('C003', 'P102_1', 5, DATE '2025-07-03'),
  ('C002', 'P101_1Temp', -5, DATE '2025-07-02'),
  ('C004', 'P104_1Temp', -5, DATE '2025-07-02');
  
  INSERT INTO `eucautomation.aks_dataset.Input_Data_2` (Product_ID, Category, Product_Name, Price)
VALUES 
  ('P100_1', 'CAT1', 'Widget A', 12.5),
  ('P101_1', 'CAT2', 'Widget B', 7.8),
  ('P102_1', 'CAT1', 'Widget C', 15.0),
  ('P101_1Temp', NULL, 'Widget X', -1),
  ('P104_1Temp', NULL, NULL, 0);
  
  INSERT INTO `eucautomation.aks_dataset.Ref_Data` (Category, Region, Department)
VALUES 
  ('CAT1', 'North', 'Tech'),
  ('CAT2', 'South', 'Hardware'),
  ('CAT3', 'East', 'Logistics'),
  ('CAT4', 'West', 'Admin');
  
  
  TRUNCATE TABLE `eucautomation.aks_dataset.config_table`;  
  INSERT INTO `eucautomation.aks_dataset.config_table` (step_id, stage, description, sql)
VALUES
  (1, 'Data Ingestion', 'Load Input Data 1', 'insert into eucautomation.aks_dataset.stage1 (SELECT * FROM `eucautomation.aks_dataset.Input_Data_1`)'),
  (2, 'Join Metadata', 'Enrich with product attributes', 
   'insert into eucautomation.aks_dataset.stage2 (SELECT a.*, b.Category, b.Product_Name, b.Price FROM `eucautomation.aks_dataset.stage1` a LEFT JOIN `eucautomation.aks_dataset.Input_Data_2` b ON a.Product_ID = b.Product_ID)'),
  (3, 'Calculation Logic', 'Calculate Total Cost', 
   'insert into eucautomation.aks_dataset.stage3 (SELECT *, Quantity * Price AS Total_Cost FROM `eucautomation.aks_dataset.stage2`)'),
  (4, 'Join Reference', 'Map Region and Department', 
   'insert into eucautomation.aks_dataset.stage4 (SELECT a.*, b.Region, b.Department FROM `eucautomation.aks_dataset.stage3` a LEFT JOIN `eucautomation.aks_dataset.Ref_Data` b ON a.Category = b.Category)'),
  (5, 'Aggregation', 'Summarize by Region, Department, Product', 
   'insert into eucautomation.aks_dataset.Final_Output (SELECT Region, Department, Product_Name, SUM(Total_Cost) AS Total_Cost FROM `eucautomation.aks_dataset.stage4` GROUP BY Region, Department, Product_Name)');

-- MANUAL STEPS
-- Step 1: Load Input Data
CREATE OR REPLACE TABLE `eucautomation.aks_dataset.stage1` AS
SELECT * FROM `eucautomation.aks_dataset.Input_Data_1`;

-- Step 2: Enrich with Product Info
CREATE OR REPLACE TABLE `eucautomation.aks_dataset.stage2` AS
SELECT a.*, b.Category, b.Product_Name, b.Price
FROM `eucautomation.aks_dataset.stage1` a
LEFT JOIN `eucautomation.aks_dataset.Input_Data_2` b
ON a.Product_ID = b.Product_ID;

-- Step 3: Calculate Total Cost
CREATE OR REPLACE TABLE `eucautomation.aks_dataset.stage3` AS
SELECT *, Quantity * Price AS Total_Cost
FROM `eucautomation.aks_dataset.stage2`;

-- Step 4: Join Reference Info
CREATE OR REPLACE TABLE `eucautomation.aks_dataset.stage4` AS
SELECT a.*, b.Region, b.Department
FROM `eucautomation.aks_dataset.stage3` a
LEFT JOIN `eucautomation.aks_dataset.Ref_Data` b
ON a.Category = b.Category;

-- Step 5: Aggregate Summary
CREATE OR REPLACE TABLE `eucautomation.aks_dataset.Final_Output` AS
SELECT Region, Department, Product_Name, SUM(Total_Cost) AS Total_Cost
FROM `eucautomation.aks_dataset.stage4`
GROUP BY Region, Department, Product_Name;

------------------------------------------------
TRUNCATE TABLE `eucautomation.aks_dataset.config_table`;  

TRUNCATE TABLE `eucautomation.aks_dataset.Input_Data_1`; 

TRUNCATE TABLE `eucautomation.aks_dataset.Input_Data_2`; 

------------------------------------------------

TRUNCATE TABLE `eucautomation.aks_dataset.stage1`;

TRUNCATE TABLE `eucautomation.aks_dataset.stage2`;

TRUNCATE TABLE `eucautomation.aks_dataset.stage3`;

TRUNCATE TABLE `eucautomation.aks_dataset.stage4`;

------------------------------------------------
TRUNCATE TABLE `eucautomation.aks_dataset.pipeline_step_logs`; 

TRUNCATE TABLE `eucautomation.aks_dataset.exception_report`; 




------------------------------------------------



SELECT
  execution_id,
  step_id,
  rule_id,
  rule_description,
  COUNT(*) AS failed_rows
FROM `eucautomation.aks_dataset.exception_report`
GROUP BY execution_id, step_id, rule_id, rule_description


