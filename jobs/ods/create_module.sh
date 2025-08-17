#!/bin/bash

read -p "Enter the new module name (e.g., ods_ocs_data): " module

if [ -z "$module" ]; then
  echo "Module name cannot be empty."
  exit 1
fi

main_dir=$(echo "$module" | sed 's/^[^_]*_//')

base_dir="${main_dir}/src/main/scala/"
test_dir="${main_dir}/src/test/scala/"
# Function to convert snake_case to CamelCase
to_camel_case() {
  local input="$1"
  echo "$input" | sed -r 's/(^|_)([a-z])/\U\2/g'
}

class_prefix=$(to_camel_case "$module")

echo "Creating directories at $base_dir"
mkdir -p "$base_dir"
mkdir -p "$test_dir"

# Main.scala
cat > "$base_dir/${class_prefix}Main.scala" << EOF
package ir.mci.dwbi.bigdata.spark_job.jobs.ods.${main_dir}

import ir.mci.dwbi.bigdata.spark_job.core.utils.BaseMain

object ${class_prefix}Main {
  def main(args: Array[String]): Unit = {
    BaseMain.run(${class_prefix}Processor.process, args)
  }
}
EOF

# Processor.scala
cat > "$base_dir/${class_prefix}Processor.scala" << EOF
package ir.mci.dwbi.bigdata.spark_job.jobs.ods.${main_dir}

import org.apache.spark.sql.DataFrame

object ${class_prefix}Processor {
  def process(df: DataFrame): DataFrame = {
    // TODO: Implement data processing logic here
    df
  }
}
EOF

# Schema.scala
cat > "$base_dir/${class_prefix}Schema.scala" << EOF
package ir.mci.dwbi.bigdata.spark_job.jobs.ods.${main_dir}

import org.apache.spark.sql.types._

object ${class_prefix}Schema {
  val schema = StructType(Seq(
  //
  ))
}
EOF

# Processor.scala
cat > "$test_dir/${class_prefix}ProcessorTest.scala" << EOF
package ir.mci.dwbi.bigdata.spark_job.jobs.ods.${main_dir}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ${class_prefix}ProcessorTest extends AnyFunSuite with Matchers {

}
EOF
echo "Module $module has been created successfully."

