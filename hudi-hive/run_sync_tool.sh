#!/usr/bin/env bash

#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.

function error_exit {
    echo "$1" >&2   ## Send message to stderr. Exclude >&2 if you don't want it that way.
    exit "${2:-1}"  ## Return a code specified by $2 or 1 by default.
}

if [ -z "${HADOOP_HOME}" ]; then
  echo "setting hadoop home dir"
  HADOOP_HOME="/usr/lib/hadoop"
fi

if [ -z "${HIVE_HOME}" ]; then
  echo "setting hive home dir"
  HIVE_HOME="/usr/lib/hive"
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#Ensure we pick the right jar even for hive11 builds
HUDI_HIVE_UBER_JAR=`ls -c $DIR/../hudi-hive-*.jar | grep -v source | head -1`

if [ -z "$HADOOP_CONF_DIR" ]; then
  echo "setting hadoop conf dir"
  HADOOP_CONF_DIR="/etc/hadoop/conf"
fi

if [ -z "$HIVE_CONF_DIR" ]; then
  echo "setting hive conf dir"
  HIVE_CONF_DIR="/etc/hive/conf"
fi

EMRFS_CONF_DIR="/usr/share/aws/emr/emrfs/conf"

## Include only specific packages from HIVE_HOME/lib to avoid version mismatches
HIVE_EXEC=`ls ${HIVE_HOME}/lib/hive-exec-*.jar | tr '\n' ':'`
HIVE_SERVICE=`ls ${HIVE_HOME}/lib/hive-service-*.jar | grep -v rpc | tr '\n' ':'`
HIVE_METASTORE=`ls ${HIVE_HOME}/lib/hive-metastore-*.jar | tr '\n' ':'`
HIVE_JDBC=`ls ${HIVE_HOME}/lib/hive-jdbc-*.jar | tr '\n' ':'`
GLUE_JARS=/usr/share/aws/hmclient/lib/aws-glue-datacatalog-hive3-client.jar:/usr/share/aws/aws-java-sdk/*

if [ -z "${HIVE_JDBC}" ]; then
  HIVE_JDBC=`ls ${HIVE_HOME}/lib/hive-jdbc-*.jar | grep -v handler | tr '\n' ':'`
fi
HIVE_JACKSON=`ls ${HIVE_HOME}/lib/jackson-*.jar | tr '\n' ':'`
HIVE_JARS=$HIVE_METASTORE:$HIVE_SERVICE:$HIVE_EXEC:$HIVE_JDBC:$HIVE_JACKSON

HADOOP_HIVE_JARS=${HIVE_JARS}:${HADOOP_HOME}/*:${HADOOP_HOME}/lib/*:/usr/lib/hadoop-hdfs/*:/usr/lib/hadoop-mapreduce/*:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*:${GLUE_JARS}

echo "Running Command : java -cp ${HADOOP_HIVE_JARS}:${HIVE_CONF_DIR}:${HADOOP_CONF_DIR}:${EMRFS_CONF_DIR}:$HUDI_HIVE_UBER_JAR org.apache.hudi.hive.HiveSyncTool $@"
java -cp $HUDI_HIVE_UBER_JAR:${HADOOP_HIVE_JARS}:${HIVE_CONF_DIR}:${HADOOP_CONF_DIR}:${EMRFS_CONF_DIR} org.apache.hudi.hive.HiveSyncTool "$@"
