-- D O C U M E N T   P R O C E S S I N G   P I P E L I N E --

USE ROLE DOC_AI_ROLE;
USE WAREHOUSE DOC_AI_XS;

-- 1 --
-- create a stage for your documents
CREATE OR REPLACE STAGE doc_ai_db.doc_ai_schema.ml_patents_stage
  DIRECTORY = (ENABLE = TRUE)
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- 2 --
-- create a stream on this stage
CREATE STREAM doc_ai_db.doc_ai_schema.ml_patents_stream ON STAGE doc_ai_db.doc_ai_schema.ml_patents_stage;
-- refresh metadata
ALTER STAGE doc_ai_db.doc_ai_schema.ml_patents_stage REFRESH;
-- check stream
select * from doc_ai_db.doc_ai_schema.ml_patents_stream;

-- 3 -- 
-- create a table to store your document information
CREATE OR REPLACE TABLE doc_ai_db.doc_ai_schema.ml_patents_review 
    (
    file_name VARCHAR,
    file_size VARIANT,
    last_modified VARCHAR,
    snowflake_file_url VARCHAR,
    json_content VARCHAR -- this contains the data extracted from the document
    );      

-- 4 --
-- create a task that checks for new files in your stage and inserts the data into your new table
CREATE OR REPLACE TASK doc_ai_db.doc_ai_schema.load_ml_patent_files
  WAREHOUSE = DOC_AI_XS -- put in your warehouse
  SCHEDULE = '5 minute'
  COMMENT = 'Process new files in the stage and insert data into the ml_patents_review table.'
WHEN SYSTEM$STREAM_HAS_DATA('doc_ai_db.doc_ai_schema.ml_patents_stream')
AS
INSERT INTO doc_ai_db.doc_ai_schema.ml_patents_review  (
  SELECT
    RELATIVE_PATH AS file_name,
    size AS file_size,
    last_modified,
    file_url AS snowflake_file_url,
    ML_PATENTS!PREDICT(GET_PRESIGNED_URL('@ml_patents_stage', RELATIVE_PATH), 1) AS json_content
  FROM doc_ai_db.doc_ai_schema.ml_patents_stream
  WHERE METADATA$ACTION = 'INSERT'
);

DESCRIBE TASK doc_ai_db.doc_ai_schema.load_ml_patent_files;

-- tasks are auto-suspended after creation
ALTER TASK doc_ai_db.doc_ai_schema.load_ml_patent_files RESUME;


-- 5 -- 
-- upload our files to the stage


-- 6 -- 
-- extract document data

-- check stream
select * from doc_ai_db.doc_ai_schema.ml_patents_stream;

-- check on task
select * from TABLE(doc_ai_db.information_schema.TASK_HISTORY(TASK_NAME => 'load_ml_patent_files'));

-- execute task (if not currently running)
execute task doc_ai_db.doc_ai_schema.load_ml_patent_files;

-- monitor task
select * from TABLE(doc_ai_db.information_schema.TASK_HISTORY(TASK_NAME => 'load_ml_patent_files'));

-- look at table
select * from doc_ai_db.doc_ai_schema.ml_patents_review;


-- 7 -- 
-- view extracted data and create views

-- header data
create or replace view doc_ai_db.doc_ai_schema.v_ml_patents_headers as 
    (
      select pr.file_name
         , pr.file_size
         , j.value:value::STRING as application_number
         , j.value:score::NUMBER(4,2) as application_number_score
         , it.value:value::STRING as invention_title
         , it.value:score::NUMBER(4,2) as invention_title_score
         , i.value:value::STRING as inventor
         , i.value:score::NUMBER(4,2) as inventor_score
         , fd.value:value::DATE as filing_dt
         , fd.value:score::NUMBER(4,2) as filing_dt_score
         , r.value:value::STRING as receipt_dttm
         , r.value:score::NUMBER(4,2) as receipt_dttm_score
         , n.value:value::INT as n_documents
         , n.value:score::NUMBER(4,2) as n_documents_score
      from doc_ai_db.doc_ai_schema.ml_patents_review pr,
      lateral flatten (input => parse_json(json_content):application_number) j,
      lateral flatten (input => parse_json(json_content):invention_title) it,
      lateral flatten (input => parse_json(json_content):inventor) i,
      lateral flatten (input => parse_json(json_content):filing_dt) fd,
      lateral flatten (input => parse_json(json_content):receipt_dttm) r,
      lateral flatten (input => parse_json(json_content):n_documents) n
    );


select *
from doc_ai_db.doc_ai_schema.v_ml_patents_headers;

-- document data
create or replace view doc_ai_db.doc_ai_schema.v_ml_patents_documents as 
    (
    select pr.file_name
         , dd.index as document_index
         , dd.value:value::STRING as document_desc
         , dd.value:score::NUMBER(4,2) as document_desc_score
         , dp.value:value::STRING as document_pages
         , dp.value:score::NUMBER(4,2) as document_pages_score
         , dt.value:value::STRING as document_title
         , dt.value:score::NUMBER(4,2) as document_title_score
      from doc_ai_db.doc_ai_schema.ml_patents_review pr,
           lateral flatten (input => parse_json(json_content):document_desc) dd,
           lateral flatten (input => parse_json(json_content):document_pages) dp,
           lateral flatten (input => parse_json(json_content):document_title) dt
     where dd.index = dp.index
           and dd.index = dt.index
    );

 
select *
from doc_ai_db.doc_ai_schema.v_ml_patents_documents;

-- 8 -- 
-- use these sqls to automate another task to write new results into materialized tables

-- set up tables
create or replace table doc_ai_db.doc_ai_schema.ml_patents_headers 
(
FILE_NAME VARCHAR,
CHECKED BOOLEAN,
FILE_SIZE NUMBER(38,0),
APPLICATION_NUMBER VARCHAR,
APPLICATION_NUMBER_SCORE NUMBER(4,2),
INVENTION_TITLE VARCHAR,
INVENTION_TITLE_SCORE NUMBER(4,2),
INVENTOR VARCHAR, 
FILING_DT DATE,
RECEIPT_DTTM VARCHAR,
N_DOCUMENTS NUMBER(38,0),
inserted_dttm DATETIME,
updated_dttm DATETIME
);

select *
  from doc_ai_db.doc_ai_schema.ml_patents_headers;


create or replace table doc_ai_db.doc_ai_schema.ml_patents_documents 
(
FILE_NAME VARCHAR,
DOCUMENT_INDEX NUMBER(38,0),
DOCUMENT_TITLE VARCHAR,
DOCUMENT_DESC  VARCHAR,
DOCUMENT_PAGES  VARCHAR,
inserted_dttm DATETIME,
updated_dttm DATETIME
);  

select *
  from doc_ai_db.doc_ai_schema.ml_patents_documents;

-- create tasks that run after loading tasks

CREATE OR REPLACE TASK doc_ai_db.doc_ai_schema.write_ml_patent_header
  WAREHOUSE = DOC_AI_XS -- put in your warehouse
  COMMENT = 'Write new lines to ml patent header table'
  AFTER doc_ai_db.doc_ai_schema.load_ml_patent_files
AS
MERGE INTO doc_ai_db.doc_ai_schema.ml_patents_headers ph 
    USING doc_ai_db.doc_ai_schema.v_ml_patents_headers vph
    ON vph.file_name = ph.file_name
WHEN NOT MATCHED THEN INSERT
    (FILE_NAME, FILE_SIZE, APPLICATION_NUMBER, APPLICATION_NUMBER_SCORE, INVENTION_TITLE, INVENTION_TITLE_SCORE, INVENTOR, FILING_DT, RECEIPT_DTTM, N_DOCUMENTS,CHECKED, inserted_dttm)
    VALUES 
    (vph.FILE_NAME, vph.FILE_SIZE, vph.APPLICATION_NUMBER, vph.APPLICATION_NUMBER_SCORE, vph.INVENTION_TITLE, vph.INVENTION_TITLE_SCORE, vph.INVENTOR, vph.FILING_DT, vph.RECEIPT_DTTM, vph.N_DOCUMENTS, FALSE, current_timestamp())
    ;

CREATE OR REPLACE TASK doc_ai_db.doc_ai_schema.write_ml_patent_document
  WAREHOUSE = DOC_AI_XS -- put in your warehouse
  COMMENT = 'Write new lines to ml patent document table'
  AFTER doc_ai_db.doc_ai_schema.load_ml_patent_files
AS
MERGE INTO doc_ai_db.doc_ai_schema.ml_patents_documents pd 
    USING doc_ai_db.doc_ai_schema.v_ml_patents_documents vpd
    ON vpd.file_name = pd.file_name and vpd.document_index = pd.document_index
WHEN NOT MATCHED THEN INSERT
    (FILE_NAME, DOCUMENT_INDEX, DOCUMENT_TITLE, DOCUMENT_DESC, DOCUMENT_PAGES, inserted_dttm)
    VALUES 
    (vpd.FILE_NAME, vpd.DOCUMENT_INDEX, vpd.DOCUMENT_TITLE, vpd.DOCUMENT_DESC, vpd.DOCUMENT_PAGES, current_timestamp());

-- activate tasks
ALTER TASK doc_ai_db.doc_ai_schema.write_ml_patent_header RESUME;
ALTER TASK doc_ai_db.doc_ai_schema.write_ml_patent_document RESUME;

-- look at tasks   
DESCRIBE TASK doc_ai_db.doc_ai_schema.write_ml_patent_header;
DESCRIBE TASK doc_ai_db.doc_ai_schema.write_ml_patent_document;

-- 9 --
-- test the whole pipeline
-- load some new files 

-- execute task
execute TASK doc_ai_db.doc_ai_schema.load_ml_patent_files;

select * from TABLE(doc_ai_db.information_schema.TASK_HISTORY(TASK_NAME => 'load_ml_patent_files'));
select * from TABLE(doc_ai_db.information_schema.TASK_HISTORY(TASK_NAME => 'write_ml_patent_header'));
select * from TABLE(doc_ai_db.information_schema.TASK_HISTORY(TASK_NAME => 'write_ml_patent_document'));

-- see what happened to our tables
select *
from doc_ai_db.doc_ai_schema.ml_patents_headers;
select *
from doc_ai_db.doc_ai_schema.ml_patents_documents;


-- 10 --
-- cleanup - maybe don't leave this task running for too long if you are not using it
-- especially if this is your own Snowflake Account
ALTER TASK doc_ai_db.doc_ai_schema.load_ml_patent_files SUSPEND;
ALTER TASK doc_ai_db.doc_ai_schema.write_ml_patent_header SUSPEND;
ALTER TASK doc_ai_db.doc_ai_schema.write_ml_patent_document SUSPEND;
DROP STREAM doc_ai_db.doc_ai_schema.ml_patents_stream;

