#!/bin/bash

source ./utils/sh/env_variable.sh

ENV=${1:-development}
SH_FILES_PATH="utils/sh"
JOB_PATH=("ods" "ods" "all_usage" "all_usage" "all_usage")
JOB_NAME=("network_switch" "pgw_new" "network_switch" "pgw_new" "cbs")
JOB_LEN=${#JOB_NAME[@]}

echo "#############################################"
echo "########--- Runnig on ${ENV} mode"
echo "#############################################"

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
