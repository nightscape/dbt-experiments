-- Create an external table backed by Avro files in HDFS
CREATE EXTERNAL TABLE my_avro_table (
  id INT,
  name STRING,
  age INT,
  email STRING
)
STORED AS AVRO
LOCATION './avro'
TBLPROPERTIES (
  'avro.schema.url'='./my-avro-record.avsc'
);

-- Verify table creation
DESCRIBE FORMATTED my_avro_table;

-- Sample query to test the table
SELECT * FROM my_avro_table LIMIT 10;

-- Error logging
SET hive.exec.log.debug.info=true;
-- Enable error logging
SET spark.sql.execution.logLevel=DEBUG;
SET spark.sql.execution.logExecution=true;

-- Insert sample data into my_avro_table
INSERT INTO my_avro_table (id, name, age, email) VALUES
(1, 'John Doe', 30, 'john.doe@example.com'),
(2, 'Jane Smith', 28, 'jane.smith@example.com'),
(3, 'Bob Johnson', 35, 'bob.johnson@example.com'),
(4, 'Alice Brown', 42, 'alice.brown@example.com'),
(5, 'Charlie Davis', 25, 'charlie.davis@example.com');

-- Verify the inserted data
SELECT * FROM my_avro_table;

-- Count the number of rows
SELECT COUNT(*) AS row_count FROM my_avro_table;

-- Query to check for any null values
SELECT 
  COUNT(*) AS total_rows,
  SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS null_ids,
  SUM(CASE WHEN name IS NULL THEN 1 ELSE 0 END) AS null_names,
  SUM(CASE WHEN age IS NULL THEN 1 ELSE 0 END) AS null_ages,
  SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS null_emails
FROM my_avro_table;

-- Print debug information
SET spark.sql.execution.explain=true;
SELECT * FROM my_avro_table WHERE age > 30;

CREATE EXTERNAL TABLE my_avro_out (
  id INT,
  name STRING,
  age INT,
  email STRING
)
STORED AS AVRO
LOCATION '/Users/martin/Workspaces/bigdata/dbt-experiments/avro-out'
TBLPROPERTIES (
  'avro.schema.url'='./my-avro-record.avsc'
);
