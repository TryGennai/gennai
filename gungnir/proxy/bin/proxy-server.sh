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

PROXY_SERVER_MAIN=org.gennai.gungnir.server.ProxyServer

PROXY_HOME=$(dirname $0)/..

PROXY_CONF_DIR=$PROXY_HOME/conf
PROXY_CONF_FILE=$PROXY_CONF_DIR/config.yaml
PROXY_CLUSTER_FILE=$PROXY_CONF_DIR/cluster.yaml
LOGBACK_CONF_PATH=$PROXY_CONF_DIR/logback.xml
PIDFILE=$PROXY_HOME/proxy-server.pid
PROXY_SERVER_HEAPSIZE=1024

if [ 2 -eq $# ]; then
  if [ `expr "x$2" : "x/"` -ne 0 ]; then
    PROXY_CONF_FILE=$2
  else
    PROXY_CONF_FILE=$PROXY_HOME/$2
  fi
fi
echo "Using config: $PROXY_CONF_FILE"

echo "Pidfile: $PIDFILE"

for file in $PROXY_HOME/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done
CLASSPATH=$CLASSPATH:$PROXY_CONF_DIR

if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

JAVA_HEAP_MAX="-Xmx""$PROXY_SERVER_HEAPSIZE""m"

case $1 in
start)
  echo -n "Starting Proxy server ... "
  if [ -f $PIDFILE ]; then
    if kill -0 `cat $PIDFILE` > /dev/null 2>&1; then
      echo already running as process `cat $PIDFILE`. 
      exit 0
    fi
  fi
  nohup $JAVA $JAVA_HEAP_MAX -Dlogback.configurationFile=$LOGBACK_CONF_PATH \
    -Dproxy.home=$PROXY_HOME -Dproxy.conf.file=$PROXY_CONF_FILE \
    -Dproxy.cluster.file=$PROXY_CLUSTER_FILE \
    -cp $CLASSPATH $PROXY_SERVER_MAIN > /dev/null 2>&1 &
  if [ $? -eq 0 ]
  then
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
reload)
  $JAVA -Dlogback.configurationFile=$LOGBACK_CONF_PATH \
    -Dproxy.home=$PROXY_HOME -Dproxy.conf.file=$PROXY_CONF_FILE \
    -Dproxy.cluster.file=$PROXY_CLUSTER_FILE \
    -cp $CLASSPATH $PROXY_SERVER_MAIN reload
  ;;
stop)
  echo -n "Stopping Proxy server ... "
  if [ ! -f "$PIDFILE" ]
  then
    echo "no Proxy server to stop (could not find file $PIDFILE)"
  else
    kill $(cat "$PIDFILE")
    rm "$PIDFILE"
    echo STOPPED
  fi
esac

