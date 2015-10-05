/**
 * Copyright 2013-2014 Recruit Technologies Co., Ltd. and contributors
 * (see CONTRIBUTORS.md)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  A copy of the
 * License is distributed with this work in the LICENSE.md file.  You may
 * also obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gennai.gungnir;

import static org.gennai.gungnir.GungnirConst.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import mockit.Deencapsulation;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import mockit.integration.junit4.JMockit;

import org.gennai.gungnir.cluster.storm.CapacityWorkerException;
import org.gennai.gungnir.cluster.storm.StormClusterManager;
import org.gennai.gungnir.cluster.storm.StormClusterManagerException;
import org.gennai.gungnir.cluster.storm.TopologyStatusChangedListener;
import org.gennai.gungnir.metastore.InMemoryMetaStore;
import org.gennai.gungnir.ql.CommandProcessor;
import org.gennai.gungnir.ql.CommandProcessorFactory;
import org.gennai.gungnir.ql.SchemaRegistry;
import org.gennai.gungnir.ql.session.StatementEntity;
import org.gennai.gungnir.topology.GungnirContext;
import org.gennai.gungnir.topology.operator.EmitOperator;
import org.gennai.gungnir.topology.operator.JoinOperator;
import org.gennai.gungnir.topology.processor.KafkaSpoutProcessor2;
import org.gennai.gungnir.topology.processor.ProcessorException;
import org.gennai.gungnir.topology.processor.spout.MessageId;
import org.gennai.gungnir.topology.processor.spout.TupleAndMessageId;
import org.gennai.gungnir.tuple.TupleValues;
import org.gennai.gungnir.tuple.persistent.BasePersistentDeserializer;
import org.gennai.gungnir.tuple.persistent.DeserializeException;
import org.gennai.gungnir.tuple.persistent.KafkaPersistentEmitter;
import org.gennai.gungnir.tuple.persistent.TrackingData;
import org.gennai.gungnir.tuple.schema.Schema;
import org.gennai.gungnir.tuple.schema.TupleSchema;
import org.gennai.gungnir.tuple.schema.ViewSchema;
import org.gennai.gungnir.utils.GungnirUtils;
import org.gennai.gungnir.utils.SnapshotJob;
import org.gennai.gungnir.utils.SnapshotTimer;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;

import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@RunWith(JMockit.class)
public class TestQueries {

  private static final Logger LOG = LoggerFactory.getLogger(TestQueries.class);

  private static final String USER_NAME = "TEST_USER";
  private static final String ACCOUNT_ID = "TEST_ACCOUNT";
  private static final String SESSION_ID = "TEST_SESSION_ID";
  private static final String STATEMENT_ID = "TEST_STATEMENT_ID";
  private static final String TEST_DIR_PREFIX = "gungnir-local-";

  private static final Pattern COMMAND = Pattern.compile("^@([^\\(\\)]+)\\((.*)\\)$",
      Pattern.DOTALL);
  private static final Pattern ANCHOR = Pattern
      .compile("^&([^\\(\\)]+)\\((.*)\\)$", Pattern.DOTALL);

  private interface CommandHandler {

    void execute(String[] args) throws Exception;
  }

  private interface CommandHook {

    boolean isMatch(String command);

    String execute(String command) throws Exception;
  }

  private Path tempPath;

  private Map<String, CommandHandler> handlersMap;
  private List<CommandHook> commandHooks;

  private GungnirManager manager;
  private StatementEntity statement;
  private CommandProcessorFactory processorFactory;
  private String result;
  private Map<String, LinkedBlockingQueue<List<Object>>> inputQueuesMap;
  private CountDownLatch started;
  private List<TrackingData> trackingData;
  private FetchStore fetchStore;
  private EmitMonitor emitMonitor;
  private Map<String, List<Map<String, Object>>> emitTuplesMap;
  private long timer;
  private Date acceptTime;
  private Period snapshotTime;

  @Before
  public void setup() throws Exception {
    Assume.assumeTrue(System.getProperty("queryTest") != null
        && "true".equalsIgnoreCase(System.getProperty("queryTest")));

    tempPath = Files.createTempDirectory(TEST_DIR_PREFIX);
  }

  @After
  public void tearDown() throws Exception {
    if (System.getProperty("queryTest") != null
        && "true".equalsIgnoreCase(System.getProperty("queryTest"))) {
      GungnirUtils.deleteDirectory(tempPath);
    }
  }

  private void addHandler(String name, CommandHandler handler) {
    handlersMap.put(name.toLowerCase(), handler);
  }

  private void setup(String queryName) throws Exception {
    manager = GungnirManager.getManager();
    manager.getConfig().put(GungnirConfig.METASTORE, InMemoryMetaStore.class.getName());
    manager.getConfig().put(GungnirConfig.CLUSTER_MODE, GungnirConfig.LOCAL_CLUSTER);
    manager.getConfig().put(GungnirConfig.LOCAL_DIR, tempPath.toString());
    UserEntity owner = new UserEntity(USER_NAME);
    owner.setId(ACCOUNT_ID);
    statement = new StatementEntity(STATEMENT_ID, SESSION_ID, owner);
    processorFactory = new CommandProcessorFactory();
    result = null;
    inputQueuesMap = Maps.newHashMap();
    started = new CountDownLatch(1);
    trackingData = Lists.newArrayList();
    fetchStore = new FetchStore();
    emitMonitor = new EmitMonitor();
    emitTuplesMap = Maps.newHashMap();
    timer = 0;
    snapshotTime = null;
    GungnirManager.getManager().getMetaStore().drop();
  }

  private String execute(String command) throws Exception {
    CommandProcessor processor = processorFactory.getProcessor(statement, command);
    if (processor != null) {
      return processor.run(statement, command);
    }
    throw new RuntimeException();
  }

  private static List<String> readFile(InputStream is) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(is));

    List<String> commands = Lists.newArrayList();
    StringBuilder sb = new StringBuilder();
    try {
      String buff = null;
      while ((buff = reader.readLine()) != null) {
        if (!buff.trim().isEmpty()) {
          if (sb.length() > 0) {
            sb.append('\n');
          }
          if (buff.charAt(buff.length() - 1) == ';') {
            sb.append(buff.substring(0, buff.length() - 1));
            commands.add(sb.toString());
            sb.setLength(0);
          } else {
            sb.append(buff);
          }
        }
      }
      return commands;
    } finally {
      reader.close();
    }
  }

  public void executeFile(String queryFileName) throws Exception {
    String queryName = queryFileName.substring(0, queryFileName.length() - 2);
    LOG.info("== Execute query({}) ============================", queryName);

    setup(queryName);

    Map<String, String> anchorsMap = Maps.newHashMap();
    List<String> commands = readFile(TestQueries.class.getResourceAsStream("/" + queryFileName));

    for (String command : commands) {
      if (command.charAt(0) == '@') {
        Matcher matcher = COMMAND.matcher(command);
        if (matcher.matches()) {
          CSVReader reader = new CSVReader(new StringReader(matcher.group(2)), ',', '\'');
          try {
            String[] args = reader.readNext();

            if (args != null) {
              for (int i = 0; i < args.length; i++) {
                if (args[i].charAt(0) == '*') {
                  String value = anchorsMap.get(args[i].substring(1));
                  if (value != null) {
                    args[i] = value;
                  }
                }
              }
            }

            CommandHandler handler = handlersMap.get(matcher.group(1).toLowerCase());
            if (handler != null) {
              handler.execute(args);
            }
          } finally {
            reader.close();
          }
        }
      } else if (command.charAt(0) == '&') {
        Matcher matcher = ANCHOR.matcher(command);
        if (matcher.matches()) {
          String name = matcher.group(1);
          String value = matcher.group(2);
          if (value.charAt(0) == '\'' && value.charAt(value.length() - 1) == '\'') {
            value = value.substring(1, value.length() - 1);
          }
          anchorsMap.put(name, value);
        }
      } else {
        CommandHook commandHook = null;
        for (CommandHook hook : commandHooks) {
          if (hook.isMatch(command)) {
            commandHook = hook;
            break;
          }
        }

        if (commandHook == null) {
          result = execute(command);
        } else {
          result = commandHook.execute(command);
        }
      }
    }
  }

  private void executeFiles(String[] queryFileNames) throws Exception {
    for (String queryFileName : queryFileNames) {
      executeFile(queryFileName);
    }
  }

  private void executeFiles() throws Exception {
    DirectoryStream<Path> ds =
        Files.newDirectoryStream(Paths.get(TestQueries.class.getResource("/")
            .getPath()), "*.q");
    for (Path p : ds) {
      Path file = p.getFileName();
      if (file != null) {
        executeFile(file.toString());
      }
    }
  }

  private void submitTopology(String command) throws Exception {
    execute(command);

    ObjectMapper mapper = new ObjectMapper();
    for (int i = 0; i < 20; i++) {
      TimeUnit.MILLISECONDS.sleep(1000);

      String res = execute("DESC TOPOLOGY");
      JsonNode descNode = mapper.readTree(res);
      if ("RUNNING".equals(descNode.get("status").asText())) {
        break;
      }
    }
  }

  private void stopTopology(String command) throws Exception {
    execute(command);

    Pattern pattern = Pattern.compile("^STOP\\s+TOPOLOGY\\s+(\\w+)$", Pattern.CASE_INSENSITIVE);
    Matcher matcher = pattern.matcher(command);
    String topologyName = null;
    if (matcher.find()) {
      topologyName = matcher.group(1);
    }

    TimeUnit.MILLISECONDS.sleep(1000);

    ObjectMapper mapper = new ObjectMapper();
    for (int i = 0; i < 20; i++) {
      TimeUnit.MILLISECONDS.sleep(1000);

      String res = execute("DESC TOPOLOGY " + topologyName);
      JsonNode descNode = mapper.readTree(res);
      if ("STOPPED".equals(descNode.get("status").asText())) {
        break;
      }
    }
  }

  private void mockStormClusterManager() {
    new MockUp<StormClusterManager>() {

      @Mock
      public void startTopology(Invocation invocation, GungnirTopology topology,
          TopologyStatusChangedListener listener) throws StormClusterManagerException,
          CapacityWorkerException {
        for (Schema schema : topology.getUsedSchemas()) {
          inputQueuesMap.put(schema.getSchemaName(), new LinkedBlockingQueue<List<Object>>());
        }

        StormClusterManager stormClusterManager = invocation.getInvokedInstance();
        stormClusterManager.startTopology(topology, listener);
      }
    };
  }

  private void mockBasePersistentDeserializer() {

    new MockUp<BasePersistentDeserializer>() {

      @Mock
      private TupleValues doDeserialize(Invocation invocation, TrackingData trackingData)
          throws DeserializeException {
        BasePersistentDeserializer deserializer = invocation.getInvokedInstance();
        TupleValues tupleValues = Deencapsulation.invoke(deserializer, "doDeserialize",
            trackingData);
        SchemaRegistry schemaRegistry = Deencapsulation.getField(deserializer, "schemaRegistry");
        Schema schema = schemaRegistry.get(tupleValues.getTupleName());
        Integer index = schema.getFieldIndex(ACCEPT_TIME_FIELD);
        if (index != null) {
          tupleValues.getValues().set(index, acceptTime);
        }
        return tupleValues;
      }
    };
  }

  private void mockKafkaPersistentEmitter() {
    new MockUp<KafkaPersistentEmitter>() {

      @Mock
      // CHECKSTYLE IGNORE MethodName FOR NEXT 1 LINES
      public void $init() {
      }

      @Mock
      public void emit(String accountId, List<TupleValues> tuples) {
        for (TupleValues tupleValues : tuples) {
          try {
            inputQueuesMap.get(tupleValues.getTupleName()).put(
                Lists.newArrayList(tupleValues.getValues()));
          } catch (InterruptedException ignore) {
            ignore = null;
          }
        }
      }

      @Mock
      public void cleanup() {
      }
    };
  }

  private void mockGungnirUtils() {
    new MockUp<GungnirUtils>() {

      @Mock
      public int currentTimeSecs() {
        return (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() + timer);
      }

      @Mock
      public long currentTimeMillis() {
        return System.currentTimeMillis() + timer;
      }
    };
  }

  private void mockKafkaSpoutProcessor2() {
    new MockUp<KafkaSpoutProcessor2>() {

      @Mock
      public void open(Invocation invocation, GungnirConfig config, GungnirContext context,
          Schema schema) throws ProcessorException {
        String schemaName = null;
        if (schema instanceof TupleSchema) {
          schemaName = schema.getSchemaName();
        } else if (schema instanceof ViewSchema) {
          schemaName = ((ViewSchema) schema).getTupleSchema().getSchemaName();
        }
        Deencapsulation.setField(invocation.getInvokedInstance(), "topicName", schemaName);

        started.countDown();
      }

      @Mock
      public List<TupleAndMessageId> read(Invocation invocation) throws ProcessorException {
        String topicName = Deencapsulation.getField(invocation.getInvokedInstance(), "topicName");
        LinkedBlockingQueue<List<Object>> queue = inputQueuesMap.get(topicName);
        try {
          List<Object> values = queue.take();
          List<TupleAndMessageId> tuples = Lists.newArrayList();
          tuples.add(new TupleAndMessageId(values, null));
          return tuples;
        } catch (InterruptedException ignore) {
          return null;
        }
      }

      @Mock
      public void ack(MessageId messageId) {
      }

      @Mock
      public void fail(MessageId messageId) {
      }

      @Mock
      public void close() {
      }
    };
  }

  private void mockJoinOperator() {
    new MockUp<JoinOperator>() {

      @Mock
      public void prepare(Invocation invocation) {
        JoinOperator operator = invocation.getInvokedInstance();
        Deencapsulation.setField(operator, "processor",
            new MockFetchProcessor(operator.getName(), fetchStore));
        Deencapsulation.invoke(operator, "prepare");
      }
    };
  }

  private void mockEmitOperator() {
    new MockUp<EmitOperator>() {

      @Mock
      protected void prepare(Invocation invocation) {
        EmitOperator operator = invocation.getInvokedInstance();
        Deencapsulation.setField(operator, "processor", new MockEmitProcessor(operator.getName(),
            emitMonitor));
        Deencapsulation.invoke(operator, "prepare");
      }
    };
  }

  private void mockSnapshotTimer() {
    new MockUp<SnapshotTimer>() {

      @Mock
      public synchronized void periodSchedule(Invocation invocation, Period period,
          SnapshotJob job) throws SchedulerException {
        SnapshotTimer snapshotTimer = invocation.getInvokedInstance();
        if (snapshotTime != null) {
          Deencapsulation.invoke(snapshotTimer, "periodSchedule", snapshotTime, job);
        } else {
          Deencapsulation.invoke(snapshotTimer, "periodSchedule", period, job);
        }
      }

      @Mock
      public synchronized void cronSchedule(Invocation invocation, String schedulingPattern,
          SnapshotJob job) throws SchedulerException {
        SnapshotTimer snapshotTimer = invocation.getInvokedInstance();
        if (snapshotTime != null) {
          Deencapsulation.invoke(snapshotTimer, "periodSchedule", snapshotTime, job);
        } else {
          Deencapsulation.invoke(snapshotTimer, "cronSchedule", schedulingPattern, job);
        }
      }
    };
  }

  private class FetchCommandHandler implements CommandHandler {

    private ObjectMapper mapper;

    FetchCommandHandler() {
      mapper = new ObjectMapper();
      mapper.configure(Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    }

    @Override
    public void execute(String[] args) throws Exception {
      String[] keyFieldNames = mapper.readValue(args[1], String[].class);
      Map<String, List<List<Object>>> fetchValuesMap = mapper.readValue(args[2],
          mapper.getTypeFactory().constructMapType(LinkedHashMap.class,
              mapper.getTypeFactory().uncheckedSimpleType(String.class),
              mapper.getTypeFactory().constructCollectionType(ArrayList.class,
                  mapper.getTypeFactory().constructCollectionType(ArrayList.class, Object.class))));
      fetchStore.put(args[0], keyFieldNames, fetchValuesMap);
    }
  }

  private class EmitCommandHandler implements CommandHandler {

    private ObjectMapper mapper;

    EmitCommandHandler() {
      mapper = new ObjectMapper();
      mapper.configure(Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    }

    @Override
    public void execute(String[] args) throws Exception {
      Map<String, Object> tuple = mapper.readValue(args[1], mapper.getTypeFactory()
          .constructMapType(LinkedHashMap.class, String.class, Object.class));
      List<Map<String, Object>> emitTuples = emitTuplesMap.get(args[0]);
      if (emitTuples == null) {
        emitTuples = Lists.newArrayList();
        emitTuplesMap.put(args[0], emitTuples);
      }
      emitTuples.add(tuple);
    }
  }

  private class PlayCommandHandler implements CommandHandler {

    @Override
    public void execute(String[] args) throws Exception {
      started.await();

      TimeUnit.MILLISECONDS.sleep(3000);

      int timeout = Integer.parseInt(args[0]);
      acceptTime = new Date(GungnirUtils.currentTimeMillis());

      for (List<Map<String, Object>> tuples : emitTuplesMap.values()) {
        for (Map<String, Object> tuple : tuples) {
          for (Map.Entry<String, Object> entry : tuple.entrySet()) {
            if (entry.getValue() != null && entry.getValue().equals(ACCEPT_TIME_FIELD)) {
              entry.setValue(acceptTime);
            }
          }
        }
      }

      emitMonitor.prepare(emitTuplesMap);

      for (TrackingData data : trackingData) {
        manager.dispatchTrackingData(ACCOUNT_ID, data);
      }
      trackingData.clear();

      emitMonitor.start(timeout);

      emitTuplesMap.clear();
    }
  }

  @Test
  public void testQueries() throws Exception {
    mockStormClusterManager();

    StormClusterManager stormClusterManager = StormClusterManager.getManager();
    stormClusterManager.startLocalCluster();

    mockBasePersistentDeserializer();

    mockKafkaPersistentEmitter();

    mockGungnirUtils();

    mockKafkaSpoutProcessor2();

    mockJoinOperator();

    mockEmitOperator();

    mockSnapshotTimer();

    handlersMap = Maps.newHashMap();
    commandHooks = Lists.newArrayList();

    addHandler("IS", new CommandHandler() {

      @Override
      public void execute(String[] args) {
        assertThat(result, is(args[0]));
      }
    });

    addHandler("POST", new CommandHandler() {

      @Override
      public void execute(String[] args) throws Exception {
        trackingData.add(new TrackingData(args[0], args[1]));
      }
    });

    addHandler("FETCH", new FetchCommandHandler());

    addHandler("EMIT", new EmitCommandHandler());

    addHandler("PLAY", new PlayCommandHandler());

    addHandler("SLEEP", new CommandHandler() {

      @Override
      public void execute(String[] args) throws Exception {
        TimeUnit.SECONDS.sleep(Long.parseLong(args[0]));
      }
    });

    addHandler("TIMER", new CommandHandler() {

      @Override
      public void execute(String[] args) throws Exception {
        if (args[0].charAt(0) == '+') {
          timer = TimeUnit.SECONDS.toMillis(Long.parseLong(args[0].substring(1)));
        } else {
          timer = TimeUnit.SECONDS.toMillis(Long.parseLong(args[0]));
        }
      }
    });

    addHandler("SNAPSHOT", new CommandHandler() {

      @Override
      public void execute(String[] args) throws Exception {
        snapshotTime = Period.of(Long.parseLong(args[0]), TimeUnit.SECONDS);
      }
    });

    commandHooks.add(new CommandHook() {

      private Pattern pattern = Pattern.compile("^SUBMIT\\s+TOPOLOGY\\s+\\w+$",
          Pattern.CASE_INSENSITIVE);

      @Override
      public boolean isMatch(String command) {
        return pattern.matcher(command).find();
      }

      @Override
      public String execute(String command) throws Exception {
        submitTopology(command);
        return "OK";
      }
    });

    commandHooks.add(new CommandHook() {

      private Pattern pattern = Pattern.compile("^STOP\\s+TOPOLOGY\\s+\\w+$",
          Pattern.CASE_INSENSITIVE);

      @Override
      public boolean isMatch(String command) {
        return pattern.matcher(command).find();
      }

      @Override
      public String execute(String command) throws Exception {
        stopTopology(command);
        return "OK";
      }
    });

    String fileNames = System.getProperty("q");
    if (fileNames != null) {
      String[] queryFileNames = fileNames.split("\\s*,\\s*");
      executeFiles(queryFileNames);
    } else {
      executeFiles();
    }

    stormClusterManager.shutdownLocalCluster();
  }
}
