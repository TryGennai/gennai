#!/bin/bash

# Copyright 2013-2014 Recruit Technologies Co., Ltd. and contributors
# (see CONTRIBUTORS.md)
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.  A copy of the
# License is distributed with this work in the LICENSE.md file.  You may
# also obtain a copy of the License from
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

GUNGNIR_SERVER_MAIN=org.gennai.gungnir.server.GungnirServer

GUNGNIR_HOME=$(dirname $0)/..

GUNGNIR_CONF_DIR=$GUNGNIR_HOME/conf
GUNGNIR_CONF_FILE=$GUNGNIR_CONF_DIR/gungnir.yaml
LOGBACK_CONF_PATH=$GUNGNIR_CONF_DIR/logback.xml
STORM_CONF_FILE=gungnir-storm.yaml
PIDFILE=$GUNGNIR_HOME/gungnir-server.pid
GUNGNIR_SERVER_HEAPSIZE=1024
PID_KEY_NAME=gungnir.server.pid.file

if [ 2 -eq $# ]; then
  if [ `expr "x$2" : "x/"` -ne 0 ]; then
    GUNGNIR_CONF_FILE=$2
  else
    GUNGNIR_CONF_FILE=$GUNGNIR_HOME/$2
  fi
fi

pid=$(grep "^[[:space:]]*$PID_KEY_NAME" "$GUNGNIR_CONF_FILE" | sed -e 's/.*:[[:blank:]]*//')
if [ -n "$pid" ]; then
  if [ `expr "x$pid" : "x/"` -ne 0 ]; then
    PIDFILE=$pid
  else
    PIDFILE=$GUNGNIR_HOME/$pid
  fi
fi
echo "Pidfile: $PIDFILE"

for file in $GUNGNIR_HOME/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done
CLASSPATH=$CLASSPATH:$GUNGNIR_CONF_DIR

if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

JAVA_HEAP_MAX="-Xmx""$GUNGNIR_SERVER_HEAPSIZE""m"

case $1 in
start)
  if [ -f "$GUNGNIR_CONF_FILE" ]; then
    echo "Using config file: $GUNGNIR_CONF_FILE"
  else
    echo "No such config file: $GUNGNIR_CONF_FILE"
    exit 1
  fi
  echo -n "Starting Gungnir server ... "
  if [ -f $PIDFILE ]; then
    if kill -0 `cat $PIDFILE` > /dev/null 2>&1; then
      echo already running as process `cat $PIDFILE`. 
      exit 0
    fi
  fi
  nohup $JAVA $JAVA_HEAP_MAX -Dlogback.configurationFile=$LOGBACK_CONF_PATH \
    -Dstorm.conf.file=$STORM_CONF_FILE -Dgungnir.home=$GUNGNIR_HOME \
    -Dgungnir.conf.file=$GUNGNIR_CONF_FILE \
    -cp $CLASSPATH $GUNGNIR_SERVER_MAIN > /dev/null 2>&1 &
  if [ $? -eq 0 ]; then
    if /bin/echo -n $! > "$PIDFILE"
    then
      sleep 1
      echo STARTED
    else
      echo FAILED TO WRITE PID
      exit 1
    fi
  else
    echo SERVER DID NOT START
    exit 1
  fi
  ;;
stop)
  echo -n "Stopping Gungnir server ... "
  if [ ! -f "$PIDFILE" ]; then
    echo "no Gungnir server to stop (could not find file $PIDFILE)"
  else
    kill $(cat "$PIDFILE")
    rm "$PIDFILE"
    echo STOPPED
  fi
  ;;
*)
  echo "Usage: $0 {start|stop} [config_file]" >&2
esac

