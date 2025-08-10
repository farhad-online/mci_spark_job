#!/bin/bash

if [ $# -eq 0 ]; then
  echo "Running sbt assembly command for all modules"
  sbt all_usage_network_switch/assembly
  sbt all_usage_pgw_new/assembly
  sbt all_usage_cbs/assembly
  sbt ods_network_switch/assembly
  sbt ods_pgw_new/assembly
else
  module="$1"
  echo "Running sbt assembly command for ${module} module"
  sbt ${module}/assembly
fi