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

ADDR=":7501"
ANNOUNCE="zk!127.0.0.1:2181!/test"

TEST_SERVER_MAIN=org.gennai.gungnir.server.TestServer

PROXY_HOME=$(dirname $0)/..

LOGBACK_CONF_PATH=$PROXY_HOME/conf/logback.xml
PIDFILE=$PROXY_HOME/test-server.pid
TEST_SERVER_HEAPSIZE=1024

echo "Pidfile: $PIDFILE"

for file in $PROXY_HOME/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

JAVA_HEAP_MAX="-Xmx""$TEST_SERVER_HEAPSIZE""m"

case $1 in
start)
  echo -n "Starting Test server ... "
  if [ -f $PIDFILE ]; then
    if kill -0 `cat $PIDFILE` > /dev/null 2>&1; then
      echo already running as process `cat $PIDFILE`. 
      exit 0
    fi
  fi
  nohup $JAVA $JAVA_HEAP_MAX -Dlogback.configurationFile=$LOGBACK_CONF_PATH \
    -Dproxy.home=$PROXY_HOME -cp $CLASSPATH $TEST_SERVER_MAIN $ADDR $ANNOUNCE > /dev/null 2>&1 &
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
stop)
  echo -n "Stopping Test server ... "
  if [ ! -f "$PIDFILE" ]
  then
    echo "no Test server to stop (could not find file $PIDFILE)"
  else
    kill $(cat "$PIDFILE")
    rm "$PIDFILE"
    echo STOPPED
  fi
esac

