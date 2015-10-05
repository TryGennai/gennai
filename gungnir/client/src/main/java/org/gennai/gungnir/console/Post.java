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
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.utils.GungnirUtils;
import org.gennai.gungnir.utils.SLF4JHandler;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.twitter.finagle.Http;
import com.twitter.finagle.Service;
import com.twitter.finagle.http.MediaType;
import com.twitter.finagle.util.DefaultTimer;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.FutureEventListener;
import com.twitter.util.TimeoutException;

public final class Post {

  private static final Logger LOG = LoggerFactory.getLogger(Post.class);

  private static final String USAGE = "post [-options] [JSON tuple]";
  private static final int DEFAULT_SEND_QUEUE_SIZE = 8192;
  private static final int DEFAULT_RESPONSE_TIMEOUT_SECS = 10;

  private static final class Request {

    private String jsonTuple;
    private int times;

    private Request(String jsonTuple, int times) {
      this.jsonTuple = jsonTuple;
      this.times = times;
    }
  }

  private String dest;
  private String uri;
  private int parallelism;
  private int times;
  private int queueSize;
  private int timeLimitsSecs;
  private boolean verbose;
  private boolean stats;
  private Service<HttpRequest, HttpResponse> client;
  private LinkedBlockingQueue<Request> queue;
  private ExecutorService postExecutor;
  private volatile boolean finish;
  private CountDownLatch finished;
  private long start;
  private AtomicInteger completed;
  private AtomicInteger failed;
  private AtomicInteger timeout;
  private int tenpct;

  private Post(String dest, String uri) {
    this.dest = dest;
    this.uri = uri;
  }

  public static final class Builder {

    private int parallelism = 1;
    private int times = 1;
    private int queueSize = DEFAULT_SEND_QUEUE_SIZE;
    private int timeLimitsSecs = DEFAULT_RESPONSE_TIMEOUT_SECS;
    private boolean verbose;
    private boolean stats;

    private Builder() {
    }

    public Builder parallelism(int parallelism) {
      this.parallelism = parallelism;
      return this;
    }

    public Builder times(int times) {
      this.times = times;
      return this;
    }

    public Builder queueSize(int queueSize) {
      this.queueSize = queueSize;
      return this;
    }

    public Builder timeLimitsSecs(int timeLimitsSecs) {
      this.timeLimitsSecs = timeLimitsSecs;
      return this;
    }

    public Builder verbose(boolean verbose) {
      this.verbose = verbose;
      return this;
    }

    public Builder stats(boolean stats) {
      this.stats = stats;
      return this;
    }

    public Post build(String dest, String uri) {
      Post post = new Post(dest, uri);
      post.parallelism = parallelism;
      post.times = times;
      post.queueSize = queueSize;
      post.timeLimitsSecs = timeLimitsSecs;
      post.verbose = verbose;
      post.stats = stats;
      return post;
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public int getCompleted() {
    if (completed != null) {
      return completed.get();
    }
    return 0;
  }

  public int getFailed() {
    if (failed != null) {
      return failed.get();
    }
    return 0;
  }

  public int getTimeout() {
    if (timeout != null) {
      return timeout.get();
    }
    return 0;
  }

  public double getTime() {
    return ((double) TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)) / 1000;
  }

  private void sendRequests(String jsonTuple) {
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
    ChannelBuffer content = ChannelBuffers.copiedBuffer(jsonTuple, CharsetUtil.UTF_8);
    request.headers().set(CONTENT_TYPE, MediaType.Json());
    request.headers().set(CONTENT_LENGTH, content.readableBytes());
    request.setContent(content);

    try {
      Await.ready(client.apply(request).raiseWithin(
          new Duration(TimeUnit.SECONDS.toNanos(timeLimitsSecs)),
          DefaultTimer.twitter()).addEventListener(new FutureEventListener<HttpResponse>() {

        @Override
        public void onFailure(Throwable cause) {
          LOG.error("Failed to send request", cause);
          if (failed != null) {
            failed.incrementAndGet();
          }

          if (verbose) {
            cause.printStackTrace(System.err);
          }
        }

        @Override
        public void onSuccess(HttpResponse response) {
          if (verbose) {
            System.out.println(response.getProtocolVersion() + " " + response.getStatus());
            for (Map.Entry<String, String> entry : response.headers().entries()) {
              System.out.println(entry.getKey() + ": " + entry.getValue());
            }
          }
        }
      }));
    } catch (TimeoutException e) {
      LOG.error("Send request timed out", e);

      if (timeout != null) {
        timeout.incrementAndGet();
      }

      if (verbose) {
        e.printStackTrace(System.err);
      }
    } catch (Exception e) {
      LOG.error("Failed to send request", e);

      if (failed != null) {
        failed.incrementAndGet();
      }

      if (verbose) {
        e.printStackTrace(System.err);
      }
    }
  }

  public void execute() {
    client = Http.newService(dest);
    queue = new LinkedBlockingQueue<Request>(queueSize);
    postExecutor =
        Executors.newFixedThreadPool(parallelism, GungnirUtils.createThreadFactory("PostExecutor"));
    finish = false;
    finished = new CountDownLatch(parallelism);

    if (stats) {
      completed = new AtomicInteger();
      failed = new AtomicInteger();
      timeout = new AtomicInteger();
    }

    for (int i = 0; i < parallelism; i++) {
      postExecutor.execute(new Runnable() {

        @Override
        public void run() {
          try {
            for (;;) {
              Request request = queue.poll();
              if (request != null) {
                for (int j = 0; j < request.times; j++) {
                  sendRequests(request.jsonTuple);

                  if (completed != null) {
                    int cnt = completed.incrementAndGet();
                    if (tenpct > 0 && cnt % tenpct == 0) {
                      System.out.println("Completed " + cnt + " requests");
                    }
                  }
                }
              } else {
                if (finish) {
                  break;
                }
                TimeUnit.SECONDS.sleep(1);
              }
            }
          } catch (InterruptedException ignore) {
            ignore = null;
          }

          finished.countDown();
        }
      });
    }
  }

  public void enqueue(String jsonTuple) throws InterruptedException {
    int t = times / parallelism;
    for (int i = 0; i < parallelism; i++) {
      int pt = t;
      if (i == 0) {
        pt += times % parallelism;
      }
      if (pt > 0) {
        queue.put(new Request(jsonTuple, pt));
      }
    }
  }

  public void send(String jsonTuple) throws InterruptedException {
    if (stats) {
      tenpct = times / 10 < 100 ? 100 : times / 10;
      start = System.nanoTime();
    }

    for (int i = 0; i < parallelism; i++) {
      enqueue(jsonTuple);
    }
  }

  public void send(final InputStream is) throws IOException, InterruptedException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(is));
    try {
      String jsonTuple = null;
      while ((jsonTuple = reader.readLine()) != null) {
        jsonTuple = jsonTuple.trim();
        if (!jsonTuple.isEmpty()) {
          enqueue(jsonTuple);
        }
      }
    } finally {
      reader.close();
    }
  }

  public void send(File... jsonFiles) throws IOException, InterruptedException {
    if (stats) {
      int total = 0;
      tenpct = 0;
      for (File jsonFile : jsonFiles) {
        BufferedReader reader = Files.newReader(jsonFile, Charsets.UTF_8);
        try {
          while (reader.readLine() != null) {
            total++;
          }
        } finally {
          reader.close();
        }
      }
      total *= times;
      tenpct = total / 10 < 100 ? 100 : total / 10;
      start = System.nanoTime();
    }

    for (File jsonFile : jsonFiles) {
      send(new FileInputStream(jsonFile));
    }
  }

  public void close() throws InterruptedException {
    finish = true;
    postExecutor.shutdown();

    finished.await();

    client.close();
  }

  // CHECKSTYLE IGNORE MethodLengthCheck FOR NEXT 1 LINES
  public static void main(String[] args) {
    java.util.logging.Logger logger = java.util.logging.Logger.getLogger("com.twitter");
    logger.setUseParentHandlers(false);
    logger.addHandler(new SLF4JHandler());

    Options options = new Options();

    OptionBuilder.hasArg();
    OptionBuilder.withArgName("accountid");
    OptionBuilder.withDescription("Account ID to use when posting to the tuple store server");
    options.addOption(OptionBuilder.create('a'));

    OptionBuilder.hasArg();
    OptionBuilder.withArgName("tuplename");
    OptionBuilder.withDescription("Tuple name to use when posting to the tuple store server");
    options.addOption(OptionBuilder.create('t'));

    OptionBuilder.hasArg();
    OptionBuilder.withArgName("number-of-parallelism");
    OptionBuilder.withDescription("Number of parallelism");
    options.addOption(OptionBuilder.create('p'));

    OptionBuilder.hasArg();
    OptionBuilder.withArgName("number-of-times");
    OptionBuilder.withDescription("number of times");
    options.addOption(OptionBuilder.create('n'));

    OptionBuilder.hasArg();
    OptionBuilder.withArgName("size-of-send-queue");
    OptionBuilder.withDescription("Size of send queue");
    options.addOption(OptionBuilder.create('q'));

    OptionBuilder.hasArg();
    OptionBuilder.withArgName("response-timeout");
    OptionBuilder.withDescription("Response timeout (secs)");
    options.addOption(OptionBuilder.create('l'));

    OptionBuilder.withArgName("verbose");
    OptionBuilder.withDescription("Make the operation more talkative");
    options.addOption(OptionBuilder.create('v'));

    OptionBuilder.withArgName("stats");
    OptionBuilder.withDescription("Print statistics information");
    options.addOption(OptionBuilder.create('s'));

    OptionBuilder.hasArgs();
    OptionBuilder.withArgName("filename");
    OptionBuilder.withDescription("JSON tuple from files");
    options.addOption(OptionBuilder.create('f'));

    OptionBuilder.withArgName("help");
    OptionBuilder.withDescription("Print help information");
    options.addOption(OptionBuilder.create('h'));

    String accountId = null;
    String tupleName = null;
    int parallelism = 1;
    int times = 1;
    int queueSize = DEFAULT_SEND_QUEUE_SIZE;
    int timeoutSecs = DEFAULT_RESPONSE_TIMEOUT_SECS;
    boolean verbose = false;
    boolean stats = false;
    try {
      CommandLine commandLine = new GnuParser().parse(options, args);
      if (commandLine.hasOption('h')) {
        new HelpFormatter().printHelp(USAGE, options);
        return;
      }
      if (commandLine.hasOption('a')) {
        accountId = commandLine.getOptionValue('a');
      } else {
        new HelpFormatter().printHelp(USAGE, options);
        return;
      }
      if (commandLine.hasOption('t')) {
        tupleName = commandLine.getOptionValue('t');
      } else {
        new HelpFormatter().printHelp(USAGE, options);
        return;
      }
      if (commandLine.hasOption('p')) {
        parallelism = Integer.valueOf(commandLine.getOptionValue('p'));
      }
      if (commandLine.hasOption('n')) {
        times = Integer.valueOf(commandLine.getOptionValue('n'));
      }
      if (commandLine.hasOption('q')) {
        queueSize = Integer.valueOf(commandLine.getOptionValue('q'));
      }
      if (commandLine.hasOption('l')) {
        timeoutSecs = Integer.valueOf(commandLine.getOptionValue('l'));
      }
      if (commandLine.hasOption('v')) {
        verbose = true;
      }
      if (commandLine.hasOption('s')) {
        stats = true;
      }

      GungnirConfig config = GungnirConfig.readGugnirConfig();

      String uri = GUNGNIR_REST_URI + "/" + accountId + "/" + tupleName + "/json";
      String dest = null;
      if (config.getString(CLUSTER_MODE).equals(LOCAL_CLUSTER)) {
        dest = config.getString(TUPLE_STORE_SERVER_HOST) + ':'
            + config.getInteger(TUPLE_STORE_SERVER_PORT);
      } else {
        List<String> zkServers = config.getList(CLUSTER_ZOOKEEPER_SERVERS);
        dest = "zk!" + StringUtils.join(zkServers, ",") + "!" + config.getString(GUNGNIR_NODE_PATH)
            + STORES_NODE_PATH;
      }

      if (verbose || stats) {
        System.out.println("POST " + uri + '\n');
        System.out.println("account ID:    " + accountId);
        System.out.println("tuple name:    " + tupleName);
        System.out.println("parallelism:   " + parallelism);
        System.out.println("times:         " + times);
        System.out.println("queue size:    " + queueSize);
        System.out.println("timeout(secs): " + timeoutSecs + '\n');
      }

      Post post = Post.builder().parallelism(parallelism).times(times).queueSize(queueSize)
          .timeLimitsSecs(timeoutSecs).verbose(verbose).stats(stats).build(dest, uri);
      post.execute();

      if (commandLine.getArgs().length > 0) {
        post.send(commandLine.getArgs()[0]);
      } else if (commandLine.hasOption('f')) {
        String[] jsonFileNames = commandLine.getOptionValues('f');
        File[] jsonFiles = new File[jsonFileNames.length];
        for (int i = 0; i < jsonFiles.length; i++) {
          jsonFiles[i] = new File(jsonFileNames[i]);
        }
        post.send(jsonFiles);
      } else {
        post.send(System.in);
      }

      post.close();

      if (verbose || stats) {
        System.out.println("Finished " + post.getCompleted() + " requests\n");
        System.out.println("Time(sec):          " + post.getTime());
        System.out.println("Completed requests: " + post.getCompleted());
        System.out.println("Failed requests:    " + post.getFailed());
        System.out.println("Timeout requests:   " + post.getTimeout());

      }
    } catch (ParseException e) {
      new HelpFormatter().printHelp(USAGE, options);
    } catch (IOException e) {
      LOG.error("An error occurred", e);
    } catch (InterruptedException ignore) {
      ignore = null;
    }
  }
}
