DECLARE step_counter INT64 DEFAULT 1;
DECLARE execution_id STRING;
DECLARE step_query STRING;

-- Declare reusable current step variable outside block for global scope
DECLARE current_step STRUCT<
  control_id STRING,
  control_type STRING,
  batch_layer STRING,
  region STRING,
  run_group STRING,
  exec_type STRING,
  place_holder_1 STRING,
  place_holder_2 STRING,
  step_id INT64,
  rule_id STRING,
  description STRING,
  comment STRING,
  statement STRING
>;

-- Load full config metadata into array
DECLARE config_steps ARRAY<STRUCT<
  control_id STRING,
  control_type STRING,
  batch_layer STRING,
  region STRING,
  run_group STRING,
  exec_type STRING,
  place_holder_1 STRING,
  place_holder_2 STRING,
  step_id INT64,
  rule_id STRING,
  description STRING,
  comment STRING,
  statement STRING
>>;


-- Generate unique execution ID
SET execution_id = FORMAT_TIMESTAMP('%Y%m%d%H%M%S', CURRENT_TIMESTAMP());

SET config_steps = (
  SELECT ARRAY_AGG(STRUCT(
    control_id,
    control_type,
    batch_layer,
    region,
    run_group,
    exec_type,
    place_holder_1,
    place_holder_2,
    step_id,
    rule_id,
    description,
    comment,
    statement
  ) ORDER BY step_id, rule_id)
  FROM `eucautomation.aks_dataset.config_table`
);

-- Loop through config steps
WHILE step_counter <= ARRAY_LENGTH(config_steps) DO
  BEGIN
    SET current_step = config_steps[ORDINAL(step_counter)];

    -- START Log
    INSERT INTO `eucautomation.aks_dataset.pipeline_step_logs`
    VALUES (
      execution_id,
      current_step.step_id,
      CONCAT(current_step.control_id, '-', current_step.region),
      current_step.description,
      "STARTED",
      CURRENT_TIMESTAMP(),
      NULL
    );

    -- Execute transformation or rule SQL
    SET step_query = current_step.statement;
    EXECUTE IMMEDIATE step_query;

    -- Capture exceptions with full control metadata
    EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO `eucautomation.aks_dataset.exception_report`
      SELECT
        '%s' AS execution_id,
        '%s' AS control_id,
        '%s' AS control_type,
        '%s' AS batch_layer,
        '%s' AS region,
        '%s' AS run_group,
        '%s' AS exec_type,
        '%s' AS place_holder_1,
        '%s' AS place_holder_2,
        %d AS step_id,
        '%s' AS rule_id,
        '%s' AS rule_description,
        '%s' AS comment,
        TO_JSON(t) AS failed_row,
        CURRENT_TIMESTAMP() AS timestamp
      FROM (%s) AS t
    """,
      execution_id,
      current_step.control_id,
      current_step.control_type,
      current_step.batch_layer,
      current_step.region,
      current_step.run_group,
      current_step.exec_type,
      current_step.place_holder_1,
      current_step.place_holder_2,
      current_step.step_id,
      current_step.rule_id,
      current_step.description,
      current_step.comment,
      current_step.statement
    );

    -- COMPLETED Log
    INSERT INTO `eucautomation.aks_dataset.pipeline_step_logs`
    VALUES (
      execution_id,
      current_step.step_id,
      CONCAT(current_step.control_id, '-', current_step.region),
      current_step.description,
      "COMPLETED",
      CURRENT_TIMESTAMP(),
      NULL
    );

  EXCEPTION WHEN ERROR THEN
    -- FAILED Log with error message
    INSERT INTO `eucautomation.aks_dataset.pipeline_step_logs`
    VALUES (
      execution_id,
      current_step.step_id,
      CONCAT(current_step.control_id, '-', current_step.region),
      current_step.description,
      "FAILED",
      CURRENT_TIMESTAMP(),
      @@error.message
    );
  END;

  SET step_counter = step_counter + 1;
END WHILE;