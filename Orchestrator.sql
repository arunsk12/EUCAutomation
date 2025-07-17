-- Declare control variables
DECLARE step_counter INT64 DEFAULT 1;
DECLARE execution_id STRING;
DECLARE step_query STRING;
DECLARE rule_stmt STRING;

-- Declare current step structure
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

-- Declare config_steps array
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

-- Generate execution ID
SET execution_id = FORMAT_TIMESTAMP('%Y%m%d%H%M%S', CURRENT_TIMESTAMP());

-- Load transformation steps from config_table
SET config_steps = (
  SELECT ARRAY_AGG(STRUCT(
    control_id, control_type, batch_layer, region, run_group, exec_type,
    place_holder_1, place_holder_2, step_id, rule_id, description, comment, statement
  ) ORDER BY step_id)
  FROM `eucautomation.aks_dataset.config_table`
);

-- Loop through transformation steps
WHILE step_counter <= ARRAY_LENGTH(config_steps) DO
  BEGIN
    SET current_step = config_steps[ORDINAL(step_counter)];

    -- Log START
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

    -- Execute transformation SQL
    SET step_query = current_step.statement;
    EXECUTE IMMEDIATE step_query;

    -- Loop through associated rules from config_rules
    FOR rule_record IN (
      SELECT rule_id, description, comment, statement
      FROM `eucautomation.aks_dataset.config_rules`
      WHERE step_id = current_step.step_id
    )
    DO
      SET rule_stmt = rule_record.statement;

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
        rule_record.rule_id,
        rule_record.description,
        rule_record.comment,
        rule_record.statement
      );
    END FOR;

    -- Log COMPLETED
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
    -- Log FAILED
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