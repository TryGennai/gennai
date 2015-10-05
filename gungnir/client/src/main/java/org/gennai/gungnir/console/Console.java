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

package org.gennai.gungnir.console;

import static org.gennai.gungnir.GungnirConfig.*;
import static org.gennai.gungnir.GungnirConst.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import jline.console.ConsoleReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.client.GungnirClient;
import org.gennai.gungnir.client.GungnirClient.Connection;
import org.gennai.gungnir.client.GungnirClient.Statement;
import org.gennai.gungnir.client.GungnirClientException;
import org.gennai.gungnir.console.hook.StartTopologyHook;
import org.gennai.gungnir.console.hook.StopTopologyHook;
import org.gennai.gungnir.console.hook.SubmitTopologyHook;
import org.gennai.gungnir.console.hook.UserCommandHook;
import org.gennai.gungnir.thrift.GungnirServerException;
import org.gennai.gungnir.utils.GungnirUtils;
import org.gennai.gungnir.utils.SLF4JHandler;
import org.jboss.netty.handler.codec.http.Cookie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.net.server.ServerSocketReceiver;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.AppenderBase;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;

public final class Console {

  private static final Logger LOG = LoggerFactory.getLogger(Console.class);

  private static final String[] KEYWORDS = {
      "FROM", "JOIN", "ON", "EMIT", "TO", "INTO", "EACH", "CAST", "FILTER", "EXPIRE", "STATE",
      "SLIDE", "LENGTH", "SNAPSHOT", "EVERY", "LIMIT", "FIRST", "LAST", "GROUP", "BEGIN", "END",
      "PARTITION", "BY", "STREAM", "AS", "USING", "PARALLELISM", "EXPLAIN", "EXTENDED", "CLEAR",
      "CREATE", "TUPLE", "VIEW", "PARTITIONED", "COMMENT", "DESCRIBE", "DESC", "SHOW", "TUPLES",
      "VIEWS", "FILE", "FILES", "FUNCTION", "FUNCTIONS", "ALTER", "DROP", "USER", "IDENTIFIED",
      "USERS", "UPLOAD", "STATS",
      "STRING", "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "BOOLEAN", "TIMESTAMP",
      "LIST", "MAP", "STRUCT",
      "TOPOLOGY", "SUBMIT", "STOP", "START", "TOPOLOGIES", "CLUSTER",
      "TRUE", "FALSE", "LIKE", "REGEXP", "IN", "ALL", "BETWEEN", "IS", "NULL", "AND", "OR", "NOT",
      "SECONDS", "MINUTES", "HOURS", "DAYS", "SET", "POST", "LOG", "COOKIE", "QUIT", "EXIT"
  };

  private static final String PROMPT = "gungnir> ";
  private static final String INPUTTING_PROMPT = "      -> ";
  private static final String HISTORY_FILE = ".gungnirhistory";

  interface CommandHandler {

    void prepare(ConsoleContext context);

    boolean isMatch(String command);

    void execute(String command);
  }

  private List<CommandHandler> commandHandlers;
  private String userName;
  private GungnirConfig config;
  private Statement statement;
  private ServerSocketReceiver receiver;
  private ObjectMapper mapper = new ObjectMapper();

  private Console() {
    commandHandlers = Lists.newArrayList();
  }

  private void addHandler(CommandHandler handler) {
    commandHandlers.add(handler);
  }

  private String getBuildTimeString() {
    String buildTimeString = null;
    try {
      Properties properties = new Properties();
      properties.load(Console.class.getResourceAsStream(BUILD_PROPERTIES));
      buildTimeString = properties.getProperty(GUNGNIR_BUILD_TIME);
    } catch (IOException e) {
      return "-";
    }
    return buildTimeString;
  }

  private String getAccountId() throws IOException, GungnirServerException, GungnirClientException {
    String res = statement.execute("DESC USER");
    JsonNode descNode = mapper.readTree(res);
    return descNode.get("id").asText();
  }

  private void startLogReceiver(Appender<ILoggingEvent> appender)
      throws GungnirServerException, GungnirClientException {
    LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
    appender.setContext(lc);
    appender.start();

    ch.qos.logback.classic.Logger logger = lc.getLogger(LOGGER_NAME);
    logger.addAppender(appender);
    logger.setLevel(Level.INFO);

    String host = config.getString(LOG_RECEIVER_HOST);
    if (host == null) {
      host = GungnirUtils.getLocalAddress();
    }
    int port = config.getInteger(LOG_RECEIVER_PORT);

    receiver = new ServerSocketReceiver();
    receiver.setAddress(host);
    receiver.setPort(port);
    receiver.setContext(lc);
    receiver.start();

    statement.execute("SET " + LOG_RECEIVER_HOST + " = " + host);
    statement.execute("SET " + LOG_RECEIVER_PORT + " = " + port);
  }

  private void processCommandString(String commandString) throws IOException,
      GungnirServerException, GungnirClientException {
    String accountId = getAccountId();

    System.out.println("Gungnir version " + GUNGNIR_VERSION_STRING + " build at "
        + getBuildTimeString());
    System.out.println("Welcome " + userName + " (Account ID: " + accountId + ")");

    System.out.println(statement.execute(commandString));
  }

  private void processCommandFile(String commandFileName) throws IOException,
      GungnirServerException, GungnirClientException {
    File commandFile = new File(commandFileName);
    if (commandFile.exists()) {
      String accountId = getAccountId();

      System.out.println("Gungnir version " + GUNGNIR_VERSION_STRING + " build at "
          + getBuildTimeString());
      System.out.println("Welcome " + userName + " (Account ID: " + accountId + ")");

      BufferedReader reader = null;
      try {
        reader = Files.newReader(commandFile, Charsets.UTF_8);

        String line = null;
        String command = null;
        StringBuilder sb = new StringBuilder();

        while ((line = reader.readLine()) != null) {
          line = line.trim();
          if (line.endsWith(";")) {
            sb.append(line.substring(0, line.length() - 1));
            command = sb.toString().trim();
            sb.setLength(0);

            if (command.length() > 0) {
              System.out.println(command);
              System.out.println(statement.execute(command));
            }
          } else {
            if (!line.isEmpty()) {
              sb.append(line + "\n");
            }
          }
        }
      } catch (GungnirServerException e) {
        System.err.println("FAILED: " + e.getMessage());
      } catch (GungnirClientException e) {
        System.err.println("FAILED: " + e.getMessage());
      } finally {
        if (reader != null) {
          reader.close();
        }
      }
    } else {
      System.err.println("Command file doesn't exist");
    }
  }

  private static final class ConsoleReaderAppender extends AppenderBase<ILoggingEvent> {

    private ConsoleContext context;
    private ObjectMapper mapper = new ObjectMapper();

    private ConsoleReaderAppender(ConsoleContext context) {
      this.context = context;
    }

    @Override
    protected void append(ILoggingEvent eventObject) {
      String marker = eventObject.getMarker().toString();
      if (marker.startsWith(PATH_MARKER_NAME)) {
        String msg = eventObject.getMessage();
        try {
          JsonNode pathNode = mapper.readTree(msg);
          JsonNode tupleNode = pathNode.get("tuple");
          StringBuilder sb = new StringBuilder().append(pathNode.get("source").asText())
              .append(" -> ").append(pathNode.get("target").asText()).append("  ( ")
              .append(tupleNode.get("tupleName").asText()).append(":")
              .append(tupleNode.get("values")).append(" )");
          context.addLogMessage("[" + marker.substring(PATH_MARKER_NAME.length() + 1) + "] "
              + sb.toString());
        } catch (JsonProcessingException e) {
          context.addLogMessage("[" + marker.substring(PATH_MARKER_NAME.length() + 1) + "] "
              + msg);
        } catch (IOException e) {
          context.addLogMessage("[" + marker.substring(PATH_MARKER_NAME.length() + 1) + "] "
              + msg);
        }
      } else {
        context.addLogMessage("[" + eventObject.getMarker() + "] " + eventObject.getMessage());
      }
    }
  }

  private void processInteractive() throws IOException, GungnirServerException,
      GungnirClientException {
    ConsoleReader reader = new ConsoleReaderBuilder().prompt(PROMPT).history(HISTORY_FILE)
        .completer(KEYWORDS).build();

    String accountId = getAccountId();

    reader.println("Gungnir version " + GUNGNIR_VERSION_STRING + " build at "
        + getBuildTimeString());
    reader.println("Welcome " + userName + " (Account ID: " + accountId + ")");

    ConsoleContext context = new ConsoleContext();
    context.setConfig(config);
    context.setStatement(statement);
    context.setAccountId(accountId);
    context.setReader(reader);
    for (CommandHandler handler : commandHandlers) {
      handler.prepare(context);
    }

    startLogReceiver(new ConsoleReaderAppender(context));

    String line = null;
    String command = null;
    StringBuilder sb = new StringBuilder();

    while ((line = reader.readLine()) != null) {
      if (!line.isEmpty()) {
        reader.getHistory().removeLast();
      }

      line = line.trim();
      if (line.trim().endsWith(";")) {
        sb.append(line.substring(0, line.length() - 1));
        command = sb.toString().trim();
        sb.setLength(0);

        if (command.length() > 0) {
          if ("QUIT".equalsIgnoreCase(command) || "EXIT".equalsIgnoreCase(command)) {
            break;
          }

          for (CommandHandler handler : commandHandlers) {
            if (handler.isMatch(command)) {
              handler.execute(command);
              reader.setPrompt(PROMPT);
              break;
            }
          }
        }
      } else {
        if (!line.trim().isEmpty()) {
          sb.append(line + "\n");
          reader.setPrompt(INPUTTING_PROMPT);
        }
      }
    }
  }

  public void execute(String[] args) throws IOException, GungnirServerException,
      GungnirClientException {
    Options options = new Options();

    OptionBuilder.hasArg();
    OptionBuilder.withArgName("username");
    OptionBuilder.withDescription("Username to use when connecting to the gungnir server");
    options.addOption(OptionBuilder.create('u'));

    OptionBuilder.hasArg();
    OptionBuilder.withArgName("password");
    OptionBuilder.withDescription("Password to use when connecting to the gungnir server");
    options.addOption(OptionBuilder.create('p'));

    OptionBuilder.hasArg();
    OptionBuilder.withArgName("quoted-command-string");
    OptionBuilder.withDescription("Command from command line");
    options.addOption(OptionBuilder.create('e'));

    OptionBuilder.hasArg();
    OptionBuilder.withArgName("filename");
    OptionBuilder.withDescription("Command from file");
    options.addOption(OptionBuilder.create('f'));

    OptionBuilder.withArgName("local-mode");
    OptionBuilder.withDescription("Connect to cluster of local mode");
    options.addOption(OptionBuilder.create('l'));

    OptionBuilder.withArgName("help");
    OptionBuilder.withDescription("Print help information");
    options.addOption(OptionBuilder.create('h'));

    String password = null;
    String commandString = null;
    String commandFileName = null;
    boolean local = false;
    try {
      CommandLine commandLine = new GnuParser().parse(options, args);
      if (commandLine.hasOption('h')) {
        new HelpFormatter().printHelp("gungnir", options);
        return;
      }
      if (commandLine.hasOption('u')) {
        userName = commandLine.getOptionValue('u');
      } else {
        new HelpFormatter().printHelp("gungnir", options);
        return;
      }
      if (commandLine.hasOption('p')) {
        password = commandLine.getOptionValue('p');
      } else {
        new HelpFormatter().printHelp("gungnir", options);
        return;
      }

      if (commandLine.hasOption('e')) {
        commandString = commandLine.getOptionValue('e');
      } else if (commandLine.hasOption('f')) {
        commandFileName = commandLine.getOptionValue('f');
      }

      if (commandLine.hasOption('l')) {
        local = true;
      }
    } catch (ParseException e) {
      new HelpFormatter().printHelp("gungnir", options);
      return;
    }

    System.out.println("Gungnir server connecting ...");
    System.out.flush();

    config = GungnirConfig.readGugnirConfig();
    if (local) {
      config.put(GungnirConfig.CLUSTER_MODE, GungnirConfig.LOCAL_CLUSTER);
    }

    Connection connection = null;
    try {
      connection = GungnirClient.getConnection(config, userName, password);
    } catch (GungnirServerException e) {
      System.err.println(e.getMessage());
      return;
    } catch (GungnirClientException e) {
      System.err.println(e.getMessage());
      return;
    }

    try {
      statement = connection.createStatement();

      if (commandString != null) {
        try {
          processCommandString(commandString);
        } catch (GungnirServerException e) {
          System.err.println(e.getMessage());
        } catch (GungnirClientException e) {
          System.err.println(e.getMessage());
        }
      } else if (commandFileName != null) {
        processCommandFile(commandFileName);
      } else {
        processInteractive();
      }
    } finally {
      if (connection != null) {
        connection.close();
      }

      if (receiver != null) {
        receiver.stop();
      }

      ((LoggerContext) LoggerFactory.getILoggerFactory()).stop();
    }
  }

  public static void main(String[] args) {
    java.util.logging.Logger logger = java.util.logging.Logger.getLogger("com.twitter");
    logger.setUseParentHandlers(false);
    logger.addHandler(new SLF4JHandler());

    Map<String, Cookie> cookiesMap = Maps.newLinkedHashMap();

    Console console = new Console();
    console.addHandler(new PostCommandHandler(cookiesMap));
    console.addHandler(new LogCommandHandler());
    console.addHandler(new CookieCommandHandler(cookiesMap));
    console.addHandler(new UploadCommandHandler());

    StatementHandler statementHandler = new StatementHandler();
    statementHandler.addHook(new UserCommandHook());
    statementHandler.addHook(new SubmitTopologyHook());
    statementHandler.addHook(new StopTopologyHook());
    statementHandler.addHook(new StartTopologyHook());
    console.addHandler(statementHandler);

    try {
      console.execute(args);
    } catch (IOException e) {
      LOG.error("An error occurred", e);
      e.printStackTrace(System.err);
    } catch (GungnirServerException e) {
      LOG.error("An error occurred", e);
      e.printStackTrace(System.err);
    } catch (GungnirClientException e) {
      LOG.error("An error occurred", e);
      e.printStackTrace(System.err);
    }
  }
}
