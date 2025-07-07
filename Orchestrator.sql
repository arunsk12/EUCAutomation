DECLARE step_query STRING;
DECLARE step_counter INT64 DEFAULT 1;
DECLARE execution_id STRING;

-- Load config steps
DECLARE config_steps ARRAY<STRUCT<
  step_id INT64,
  stage STRING,
  description STRING,
  sql STRING
>>;

DECLARE rule_stmt STRING;

SET execution_id = FORMAT_TIMESTAMP('%Y%m%d%H%M%S', CURRENT_TIMESTAMP());
SET config_steps = (
  SELECT ARRAY_AGG(STRUCT(step_id, stage, description, sql) ORDER BY step_id)
  FROM `eucautomation.aks_dataset.config_table`
);


-- Loop through and execute
WHILE step_counter <= ARRAY_LENGTH(config_steps) DO
  BEGIN
    -- Start log
    INSERT INTO `eucautomation.aks_dataset.pipeline_step_logs`
    VALUES (execution_id,
            config_steps[ORDINAL(step_counter)].step_id,
            config_steps[ORDINAL(step_counter)].stage,
            config_steps[ORDINAL(step_counter)].description,
            "STARTED",
            CURRENT_TIMESTAMP(),
            NULL);

    -- Execute step
    SET step_query = config_steps[ORDINAL(step_counter)].sql;
    EXECUTE IMMEDIATE step_query;
	
	-- Step: Execute associated rules

FOR rule_record IN (
  SELECT rule_id, description, comment, statement
  FROM `eucautomation.aks_dataset.config_rules`
  WHERE step_id = config_steps[ORDINAL(step_counter)].step_id
)
DO
  SET rule_stmt = rule_record.statement;

EXECUTE IMMEDIATE FORMAT("""
  INSERT INTO `eucautomation.aks_dataset.exception_report`
  SELECT
    '%s' AS execution_id,
    %d AS step_id,
    '%s' AS rule_id,
    '%s' AS rule_description,
    '%s' AS comment,
    TO_JSON(t) AS failed_row,
    CURRENT_TIMESTAMP() AS timestamp
  FROM (%s) AS t
""",
  execution_id,
  config_steps[ORDINAL(step_counter)].step_id,
  rule_record.rule_id,
  rule_record.description,
  rule_record.comment,
  rule_record.statement);
END FOR;


-- Success log
INSERT INTO `eucautomation.aks_dataset.pipeline_step_logs`
VALUES (execution_id,
        config_steps[ORDINAL(step_counter)].step_id,
        config_steps[ORDINAL(step_counter)].stage,
        config_steps[ORDINAL(step_counter)].description,
        "COMPLETED",
        CURRENT_TIMESTAMP(),
        NULL);

EXCEPTION WHEN ERROR THEN
-- Failure log
INSERT INTO `eucautomation.aks_dataset.pipeline_step_logs`
VALUES (execution_id,
        config_steps[ORDINAL(step_counter)].step_id,
        config_steps[ORDINAL(step_counter)].stage,
        config_steps[ORDINAL(step_counter)].description,
        "FAILED",
        CURRENT_TIMESTAMP(),
        @@error.message);
END;

SET step_counter = step_counter + 1;

END WHILE;

