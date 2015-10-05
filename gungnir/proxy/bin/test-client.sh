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

DEST=":7400"
URI="/test1/test2"
PARALLELISM=10
TIMES=100

TEST_CLIENT_MAIN=org.gennai.gungnir.server.TestClient

PROXY_HOME=$(dirname $0)/..

LOGBACK_CONF_PATH=$PROXY_HOME/conf/logback.xml
TEST_CLIENT_HEAPSIZE=1024

for file in $PROXY_HOME/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

JAVA_HEAP_MAX="-Xmx""$TEST_CLIENT_HEAPSIZE""m"

$JAVA $JAVA_HEAP_MAX -Dlogback.configurationFile=$LOGBACK_CONF_PATH \
  -Dproxy.home=$PROXY_HOME -cp $CLASSPATH $TEST_CLIENT_MAIN $DEST $URI $PARALLELISM $TIMES

