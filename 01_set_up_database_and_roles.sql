-- S E T   U P   R O L E S   A N D   D A T A B A S E S --

USE ROLE ACCOUNTADMIN;

-- create an xs (doc_ai_xs) and an m warehouse (doc_ai_m)
CREATE OR REPLACE WAREHOUSE doc_ai_xs WITH
    WAREHOUSE_SIZE = xsmall
    AUTO_SUSPEND = 60 -- default is 600
    INITIALLY_SUSPENDED = true; 

CREATE OR REPLACE WAREHOUSE doc_ai_m WITH
    WAREHOUSE_SIZE = medium
    AUTO_SUSPEND = 60 -- default is 600
    INITIALLY_SUSPENDED = true; 

USE WAREHOUSE doc_ai_xs;

-- 1 --
--create a database and schema for your Document AI build
CREATE DATABASE doc_ai_db;
CREATE SCHEMA doc_ai_db.doc_ai_schema;

-- 2 --
-- create a custom role for building Document AI models
USE ROLE ACCOUNTADMIN;
CREATE ROLE doc_ai_role;


-- 3 --
-- equip your new role with the necessary privileges

-- SNOWFLAKE.DOCUMENT_INTELLIGENCE_CREATOR role
GRANT DATABASE ROLE SNOWFLAKE.DOCUMENT_INTELLIGENCE_CREATOR TO ROLE doc_ai_role;


-- warehouse usage
GRANT USAGE, OPERATE ON WAREHOUSE doc_ai_xs TO ROLE doc_ai_role;
GRANT USAGE, OPERATE ON WAREHOUSE doc_ai_m TO ROLE doc_ai_role;


-- database & schema rights
GRANT USAGE ON DATABASE doc_ai_db TO ROLE doc_ai_role;
GRANT USAGE ON SCHEMA doc_ai_db.doc_ai_schema TO ROLE doc_ai_role;

-- stage rights
GRANT CREATE STAGE ON SCHEMA doc_ai_db.doc_ai_schema TO ROLE doc_ai_role;

-- Document AI rights on schema
GRANT CREATE SNOWFLAKE.ML.DOCUMENT_INTELLIGENCE ON SCHEMA doc_ai_db.doc_ai_schema TO ROLE doc_ai_role;

-- pipelines, streams & tasks
GRANT CREATE STREAM, CREATE TABLE, CREATE TASK, CREATE VIEW ON SCHEMA doc_ai_db.doc_ai_schema TO ROLE doc_ai_role;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE doc_ai_role;

-- 4 --
-- grant the role to your user
GRANT ROLE doc_ai_role TO USER <>; -- adapt your user name here!