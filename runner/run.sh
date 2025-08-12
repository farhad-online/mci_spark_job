#!/bin/bash

source utils/sh/env_variable.sh
source utils/sh/job_config_generator.sh
source utils/sh/job_runner_generator.sh

ENV=${1:-development}
SH_FILES_PATH="utils/sh"
JOB_PATH=("ods" "ods" "all_usage" "all_usage" "all_usage")
JOB_NAME=("network_switch" "pgw_new" "network_switch" "pgw_new" "cbs")
JOB_LEN=${#JOB_NAME[@]}

echo "#############################################"
echo "########--- Runnig on ${ENV} mode"
if [ ${ENV} = "production" ]; then
  setup_production_variable
elif [ ${ENV} = "test" ]; then
  setup_test_variable
else
  setup_development_variable
fi
echo "#############################################"

echo ${APP_ENV}

echo "#############################################"
echo "########--- Building modules"
./${SH_FILES_PATH}/build.sh
echo "#############################################"

echo "#############################################"
for ((i=0; i<JOB_LEN; i++)); do
  echo "########--- Generate runner and config file for: ${JOB_PATH[i]}.${JOB_NAME[i]}"
  ./${SH_FILES_PATH}/job_runner_generator.sh ${ENV} ${JOB_PATH[i]} ${JOB_NAME[i]}
  ./${SH_FILES_PATH}/job_config_generator.sh ${ENV} ${JOB_PATH[i]} ${JOB_NAME[i]}
  done
echo "#############################################"
