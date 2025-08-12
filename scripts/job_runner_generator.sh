#!/bin/bash

generate_from_template() {
  local template='
#!/bin/bash

JAR_PATH="../../../jars"
KEYTAB_PATH="../../../keytabs"
CONFIG_PATH="../../../config"
JOB_PATH="."
JOB_NAME="all_usage_cbs"
HDFS_JOB_CONFIG_PATH="/user/spark/jobs/all_usage/cbs"
HDFS_CONFIG_PATH="${HDFS_JOB_CONFIG_PATH}/${JOB_NAME}.conf"

ln -sf ${KEYTAB_PATH}/*
ln -sf ${CONFIG_PATH{/*

sudo -u hdfs hdfs dfs -mkdir -p ${HDFS_JOB_CONFIG_PATH}
sudo -u hdfs hdfs dfs -rm ${HDFS_CONFIG_PATH}
sudo -u hdfs hdfs dfs -put ${JOB_PATH}/${JOB_NAME}.conf ${HDFS_CONFIG_PATH}
sudo -u hdfs hdfs dfs -put ${CONFIG_PATH}/spark_jaas.conf ${HDFS_BASE_CONFIG_PATH}/spark_jaas.conf

sudo -u spark spark-submit \
        --deploy-mode cluster \
        --jars ${JAR_PATH}/config-1.4.1.jar,${JAR_PATH}/hive-serde-3.1.3.jar,${JAR_PATH}/com.github.luben_zstd-jni-1.4.8-1.jar,${JAR_PATH}/org.apache.commons_commons-pool2-2.6.2.jar,${JAR_PATH}/org.apache.kafka_kafka-clients-2.6.0.jar,${JAR_PATH}/org.apache.spark_spark-sql-kafka-0-10_2.12-3.1.3.jar,${JAR_PATH}/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.1.3.jar,${JAR_PATH}/org.lz4_lz4-java-1.7.1.jar,${JAR_PATH}/org.slf4j_slf4j-api-1.7.30.jar,${JAR_PATH}/org.spark-project.spark_unused-1.0.0.jar,${JAR_PATH}/org.xerial.snappy_snappy-java-1.1.8.2.jar,${JAR_PATH}/log4j-core-2.21.1.jar,${JAR_PATH}/log4j-api-2.21.1.jar,${JAR_PATH}/log4j-slf4j-impl-2.21.1.jar \
        --files kafka.service.keytab,jaas.conf,${JOB_NAME}.conf \
        --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=jaas.conf" \
        --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=jaas.conf" \
        --keytab spark.service.keytab \
        --principal spark \
        --conf spark.yarn.queue=default \
        ${JOB_NAME}.jar ${HDFS_CONFIG_PATH}
'

    echo "$template" | \
        APP_NAME="$app_name" \
        PORT="$port" \
        ENVIRONMENT="$environment" \
        envsubst > "$output_path"

    chmod +x "$output_path"
}