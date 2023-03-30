-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Global test variables
\set IS_DISTRIBUTED FALSE
\set IS_TIME_DIMENSION_WITH_TIMEZONE_1ST FALSE
\set IS_TIME_DIMENSION_WITH_TIMEZONE_2TH FALSE
\set IS_JOIN TRUE
-- ########################################################
-- ## INTEGER data type tests
-- ########################################################

-- Current test variables
\set IS_TIME_DIMENSION FALSE
\set TIME_DIMENSION_DATATYPE INTEGER
\set CAGG_NAME_1ST_LEVEL conditions_summary_1_1
\set CAGG_NAME_2TH_LEVEL conditions_summary_2_5
\set CAGG_NAME_3TH_LEVEL conditions_summary_3_10

--
-- Run common tests for INTEGER
--
\set BUCKET_WIDTH_1ST 'INTEGER \'1\''
\set BUCKET_WIDTH_2TH 'INTEGER \'5\''
\set BUCKET_WIDTH_3TH 'INTEGER \'10\''

-- Different order of time dimension in raw ht
\set IS_DEFAULT_COLUMN_ORDER FALSE
\ir include/cagg_on_cagg_setup.sql
\ir include/cagg_on_cagg_common.sql

-- Default tests
\set ON_ERROR_STOP 0
\set IS_DEFAULT_COLUMN_ORDER TRUE
\ir include/cagg_on_cagg_setup.sql
\ir include/cagg_on_cagg_common.sql

--
-- Validation test for non-multiple bucket sizes
--
\set ON_ERROR_STOP 0
\set BUCKET_WIDTH_1ST 'INTEGER \'2\''
\set BUCKET_WIDTH_2TH 'INTEGER \'5\''
\set WARNING_MESSAGE '-- SHOULD ERROR because non-multiple bucket sizes'
\ir include/cagg_on_cagg_validations.sql

--
-- Validation test for equal bucket sizes
--
\set ON_ERROR_STOP 0
\set BUCKET_WIDTH_1ST 'INTEGER \'2\''
\set BUCKET_WIDTH_2TH 'INTEGER \'2\''
\set WARNING_MESSAGE 'SHOULD WORK because new bucket should be greater than previous'
\ir include/cagg_on_cagg_validations.sql

--
-- Validation test for bucket size less than source
--
\set ON_ERROR_STOP 0
\set BUCKET_WIDTH_1ST 'INTEGER \'4\''
\set BUCKET_WIDTH_2TH 'INTEGER \'2\''
\set WARNING_MESSAGE '-- SHOULD ERROR because new bucket should be greater than previous'
\ir include/cagg_on_cagg_validations.sql

-- ########################################################
-- ## TIMESTAMP data type tests
-- ########################################################

-- Current test variables
\set IS_TIME_DIMENSION TRUE
\set TIME_DIMENSION_DATATYPE TIMESTAMP
\set CAGG_NAME_1ST_LEVEL conditions_summary_1_hourly
\set CAGG_NAME_2TH_LEVEL conditions_summary_2_daily
\set CAGG_NAME_3TH_LEVEL conditions_summary_3_weekly
\set IS_JOIN TRUE
SET timezone TO 'UTC';

--
-- Run common tests for TIMESTAMP
--
\set BUCKET_WIDTH_1ST 'INTERVAL \'1 hour\''
\set BUCKET_WIDTH_2TH 'INTERVAL \'1 day\''
\set BUCKET_WIDTH_3TH 'INTERVAL \'1 week\''

-- Different order of time dimension in raw ht
\set IS_DEFAULT_COLUMN_ORDER FALSE
\ir include/cagg_on_cagg_setup.sql
\ir include/cagg_on_cagg_common.sql

-- Default tests
\set IS_DEFAULT_COLUMN_ORDER TRUE
\ir include/cagg_on_cagg_setup.sql
\ir include/cagg_on_cagg_common.sql

--
-- Validation test for variable bucket on top of fixed bucket
--
\set ON_ERROR_STOP 0
\set BUCKET_WIDTH_1ST 'INTERVAL \'1 month\''
\set BUCKET_WIDTH_2TH 'INTERVAL \'60 days\''
\set WARNING_MESSAGE '-- SHOULD ERROR because is not allowed variable-size bucket on top of fixed-size bucket'
\ir include/cagg_on_cagg_validations.sql

--
-- Validation test for non-multiple bucket sizes
--
\set ON_ERROR_STOP 0
\set BUCKET_WIDTH_1ST 'INTERVAL \'2 hours\''
\set BUCKET_WIDTH_2TH 'INTERVAL \'3 hours\''
\set WARNING_MESSAGE '-- SHOULD ERROR because non-multiple bucket sizes'
\ir include/cagg_on_cagg_validations.sql

--
-- Validation test for equal bucket sizes
--
\set ON_ERROR_STOP 0
\set BUCKET_WIDTH_1ST 'INTERVAL \'1 hour\''
\set BUCKET_WIDTH_2TH 'INTERVAL \'1 hour\''
\set WARNING_MESSAGE 'SHOULD WORK because new bucket should be greater than previous'
\ir include/cagg_on_cagg_validations.sql

--
-- Validation test for bucket size less than source
--
\set ON_ERROR_STOP 0
\set BUCKET_WIDTH_1ST 'INTERVAL \'2 hours\''
\set BUCKET_WIDTH_2TH 'INTERVAL \'1 hour\''
\set WARNING_MESSAGE '-- SHOULD ERROR because new bucket should be greater than previous'
\ir include/cagg_on_cagg_validations.sql

-- ########################################################
-- ## TIMESTAMPTZ data type tests
-- ########################################################

-- Current test variables
\set IS_TIME_DIMENSION TRUE
\set TIME_DIMENSION_DATATYPE TIMESTAMPTZ
\set CAGG_NAME_1ST_LEVEL conditions_summary_1_hourly
\set CAGG_NAME_2TH_LEVEL conditions_summary_2_daily
\set CAGG_NAME_3TH_LEVEL conditions_summary_3_weekly

SET timezone TO 'UTC';

--
-- Run common tests for TIMESTAMPTZ
--
\set BUCKET_WIDTH_1ST 'INTERVAL \'1 hour\''
\set BUCKET_WIDTH_2TH 'INTERVAL \'1 day\''
\set BUCKET_WIDTH_3TH 'INTERVAL \'1 week\''

-- Different order of time dimension in raw ht
\set ON_ERROR_STOP 0
\set IS_DEFAULT_COLUMN_ORDER FALSE
\ir include/cagg_on_cagg_setup.sql
\ir include/cagg_on_cagg_common.sql

-- Default tests
\set ON_ERROR_STOP 0
\set IS_DEFAULT_COLUMN_ORDER TRUE
\ir include/cagg_on_cagg_setup.sql
\ir include/cagg_on_cagg_common.sql

--
-- Validation test for variable bucket on top of fixed bucket
--
\set ON_ERROR_STOP 0
\set BUCKET_WIDTH_1ST 'INTERVAL \'1 month\''
\set BUCKET_WIDTH_2TH 'INTERVAL \'60 days\''
\set WARNING_MESSAGE '-- SHOULD ERROR because is not allowed variable-size bucket on top of fixed-size bucket'
\ir include/cagg_on_cagg_validations.sql

--
-- Validation test for non-multiple bucket sizes
--
\set ON_ERROR_STOP 0
\set BUCKET_WIDTH_1ST 'INTERVAL \'2 hours\''
\set BUCKET_WIDTH_2TH 'INTERVAL \'3 hours\''
\set WARNING_MESSAGE '-- SHOULD ERROR because non-multiple bucket sizes'
\ir include/cagg_on_cagg_validations.sql

--
-- Validation test for equal bucket sizes
--
\set ON_ERROR_STOP 0
\set BUCKET_WIDTH_1ST 'INTERVAL \'1 hour\''
\set BUCKET_WIDTH_2TH 'INTERVAL \'1 hour\''
\set WARNING_MESSAGE 'SHOULD WORK because new bucket should be greater than previous'
\ir include/cagg_on_cagg_validations.sql

--
-- Validation test for bucket size less than source
--
\set ON_ERROR_STOP 0
\set BUCKET_WIDTH_1ST 'INTERVAL \'2 hours\''
\set BUCKET_WIDTH_2TH 'INTERVAL \'1 hour\''
\set WARNING_MESSAGE '-- SHOULD ERROR because new bucket should be greater than previous'
\ir include/cagg_on_cagg_validations.sql

--
-- Validations using time bucket with timezone (ref issue #5126)
--
\set ON_ERROR_STOP 0
\set TIME_DIMENSION_DATATYPE TIMESTAMPTZ
\set IS_TIME_DIMENSION_WITH_TIMEZONE_1ST TRUE
\set IS_TIME_DIMENSION_WITH_TIMEZONE_2TH TRUE
\set CAGG_NAME_1ST_LEVEL conditions_summary_1_5m
\set CAGG_NAME_2TH_LEVEL conditions_summary_2_1h
\set BUCKET_TZNAME_1ST 'US/Pacific'
\set BUCKET_TZNAME_2TH 'US/Pacific'
\set BUCKET_WIDTH_1ST 'INTERVAL \'5 minutes\''
\set BUCKET_WIDTH_2TH 'INTERVAL \'1 hour\''
\set WARNING_MESSAGE '-- SHOULD WORK'
\ir include/cagg_on_cagg_validations.sql

\set BUCKET_WIDTH_1ST 'INTERVAL \'5 minutes\''
\set BUCKET_WIDTH_2TH 'INTERVAL \'16 minutes\''
\set WARNING_MESSAGE '-- SHOULD ERROR because non-multiple bucket sizes'
\ir include/cagg_on_cagg_validations.sql

--
-- Variable bucket size with the same timezones
--
\set BUCKET_TZNAME_1ST 'UTC'
\set BUCKET_TZNAME_2TH 'UTC'
\set BUCKET_WIDTH_1ST 'INTERVAL \'1 day\''
\set BUCKET_WIDTH_2TH 'INTERVAL \'1 month\''
\set WARNING_MESSAGE '-- SHOULD WORK'
\ir include/cagg_on_cagg_validations.sql

--
-- Variable bucket size with different timezones
--
\set BUCKET_TZNAME_1ST 'US/Pacific'
\set BUCKET_TZNAME_2TH 'UTC'
\set BUCKET_WIDTH_1ST 'INTERVAL \'1 day\''
\set BUCKET_WIDTH_2TH 'INTERVAL \'1 month\''
\set WARNING_MESSAGE '-- SHOULD WORK'
\ir include/cagg_on_cagg_validations.sql

--
-- TZ bucket on top of non-TZ bucket
--
\set IS_TIME_DIMENSION_WITH_TIMEZONE_1ST FALSE
\set IS_TIME_DIMENSION_WITH_TIMEZONE_2TH TRUE
\set BUCKET_TZNAME_2TH 'UTC'
\set BUCKET_WIDTH_1ST 'INTERVAL \'1 day\''
\set BUCKET_WIDTH_2TH 'INTERVAL \'1 month\''
\set WARNING_MESSAGE '-- SHOULD WORK'
\ir include/cagg_on_cagg_validations.sql

--
-- non-TZ bucket on top of TZ bucket
--
\set IS_TIME_DIMENSION_WITH_TIMEZONE_1ST TRUE
\set IS_TIME_DIMENSION_WITH_TIMEZONE_2TH FALSE
\set BUCKET_TZNAME_1ST 'UTC'
\set BUCKET_WIDTH_1ST 'INTERVAL \'1 day\''
\set BUCKET_WIDTH_2TH 'INTERVAL \'1 month\''
\set WARNING_MESSAGE '-- SHOULD WORK'
\ir include/cagg_on_cagg_validations.sql

-- test some intuitive intervals that should work but
-- were not working due to unix epochs
-- validation test for 1 year on top of one day
-- validation test for 1 year on top of 1 month
-- validation test for 1 year on top of 1 week

-- bug report 5231
\set BUCKET_WIDTH_1ST 'INTERVAL \'1 day\''
\set BUCKET_WIDTH_2TH 'INTERVAL \'1 year\''
\ir include/cagg_on_cagg_validations.sql

\set BUCKET_WIDTH_1ST 'INTERVAL \'1 day\''
\set BUCKET_WIDTH_2TH 'INTERVAL \'3 month\''
\ir include/cagg_on_cagg_validations.sql

\set BUCKET_WIDTH_1ST 'INTERVAL \'1 month\''
\set BUCKET_WIDTH_2TH 'INTERVAL \'1 year\''
\ir include/cagg_on_cagg_validations.sql

\set BUCKET_WIDTH_1ST 'INTERVAL \'1 week\''
\set BUCKET_WIDTH_2TH 'INTERVAL \'1 year\''
\ir include/cagg_on_cagg_validations.sql

\set BUCKET_WIDTH_1ST 'INTERVAL \'1 week\''
\set BUCKET_WIDTH_2TH 'INTERVAL \'1 month\''
\ir include/cagg_on_cagg_validations.sql
