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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#Ensure we pick the right jar even for hive11 builds
HOODIE_JAR=`ls -c $DIR/../hudi-timeline-server-bundle-*.jar | grep -v test | head -1`

if [ -z "$HADOOP_HOME" ]; then
  echo "setting hadoop home dir"
  HADOOP_HOME="/usr/lib/hadoop"
fi

if [ -z "$HADOOP_CONF_DIR" ]; then
  echo "setting hadoop conf dir"
  HADOOP_CONF_DIR="/etc/hadoop/conf"
fi

EMRFS_CONF_DIR="/usr/share/aws/emr/emrfs/conf"

if [ ! -f "${DIR}/javax.servlet-api-3.1.0.jar" ]; then
 echo "Downloading runtime servlet jar first :"
 sudo wget https://repo1.maven.org/maven2/javax/servlet/javax.servlet-api/3.1.0/javax.servlet-api-3.1.0.jar
fi

if [ ! -f "${DIR}/javax.servlet-api-3.1.0.jar" ]; then
 echo "Servlet API Jar not found. "
 exit -1
fi

HADOOP_COMMON_JARS=`ls -c ${HADOOP_HOME}/*.jar | grep -v test | grep -v source | grep -v servlet | grep -v 'jetty-' | grep -v 'jersey-' | grep -v 'jsp-' | tr '\n' ':'`
HADOOP_COMMON_LIB_JARS=`ls -c ${HADOOP_HOME}/lib/*.jar | grep -v test | grep -v source | grep -v servlet | grep -v 'jetty-' | grep -v 'jersey-' | grep -v 'jsp-' | tr '\n' ':'`
HADOOP_HDFS_JARS=`ls -c /usr/lib/hadoop-hdfs/*.jar | grep -v test | grep -v source | grep -v servlet | grep -v 'jetty-' | grep -v 'jersey-' | grep -v 'jsp-' | tr '\n' ':'`
HADOOP_HDFS_LIB_JARS=`ls -c /usr/lib/hadoop-hdfs/lib/*.jar | grep -v test | grep -v source | grep -v servlet | grep -v 'jetty-' | grep -v 'jersey-' | grep -v 'jsp-' | tr '\n' ':'`
EMRFS_JARS=`ls -c /usr/share/aws/emr/emrfs/lib/*.jar /usr/share/aws/emr/emrfs/auxlib/*.jar | tr '\n' ':'`
HADOOP_JARS=${HADOOP_COMMON_JARS}:${HADOOP_COMMON_LIB_JARS}:${HADOOP_HDFS_JARS}:${HADOOP_HDFS_LIB_JARS}:${EMRFS_JARS}

SERVLET_JAR=${DIR}/javax.servlet-api-3.1.0.jar
echo "Running command : java -cp ${SERVLET_JAR}:${HADOOP_JARS}:${HADOOP_CONF_DIR}:${EMRFS_CONF_DIR}:$HOODIE_JAR org.apache.hudi.timeline.service.TimelineService $@"
java -Xmx4G -cp ${SERVLET_JAR}:${HADOOP_JARS}:${HADOOP_CONF_DIR}:${EMRFS_CONF_DIR}:$HOODIE_JAR org.apache.hudi.timeline.service.TimelineService "$@"
