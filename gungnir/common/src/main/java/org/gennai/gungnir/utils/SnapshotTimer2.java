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

package org.gennai.gungnir.utils;

import static org.gennai.gungnir.GungnirConst.*;
import static org.quartz.DateBuilder.*;
import static org.quartz.JobBuilder.*;
import static org.quartz.SimpleScheduleBuilder.*;
import static org.quartz.TriggerBuilder.*;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.gennai.gungnir.Period;
import org.gennai.gungnir.utils.SnapshotJob.SnapshotTask;
import org.quartz.CronScheduleBuilder;
import org.quartz.DateBuilder.IntervalUnit;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.ScheduleBuilder;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.SimpleTrigger;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class SnapshotTimer2 {

  private static final Logger LOG = LoggerFactory.getLogger(SnapshotTimer2.class);

  public static class JobCaller implements Job {

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
      @SuppressWarnings("unchecked")
      LinkedBlockingQueue<SnapshotTask> taskQueue =
          (LinkedBlockingQueue<SnapshotTask>) context.getJobDetail().getJobDataMap()
              .get("queue");
      SnapshotJob job = (SnapshotJob) context.getJobDetail().getJobDataMap().get("job");

      for (SnapshotTask task : job.getTasks()) {
        try {
          taskQueue.put(task);
        } catch (InterruptedException e) {
          LOG.info("Task queue interrupted");
        }
      }
    }
  }

  private int queueSize;
  private int parallelism;
  private Scheduler scheduler;
  private LinkedBlockingQueue<SnapshotTask> taskQueue;
  private ExecutorService snapshotExecutor;
  private List<SnapshotCaller> callers;

  private class SnapshotCaller implements Runnable {

    @Override
    public void run() {
      try {
        while (!Thread.interrupted()) {
          SnapshotTask task = taskQueue.take();
          task.execute();
        }
      } catch (InterruptedException e) {
        LOG.info("Snapshot timer interrupted");
      }

      if (!taskQueue.isEmpty()) {
        for (Iterator<SnapshotTask> it = taskQueue.iterator(); !Thread.interrupted()
            && it.hasNext();) {
          SnapshotTask task = it.next();
          task.execute();
        }
      }
    }
  }

  public SnapshotTimer2(int queueSize, int parallelism) {
    this.queueSize = queueSize;
    this.parallelism = parallelism;
  }

  private JobDetail createJob(SnapshotJob job) throws SchedulerException {
    if (scheduler == null) {
      SchedulerFactory schedulerFactory = new StdSchedulerFactory();
      scheduler = schedulerFactory.getScheduler();
      scheduler.start();

      taskQueue = new LinkedBlockingQueue<SnapshotTask>(queueSize);
      snapshotExecutor = Executors.newCachedThreadPool(
          GungnirUtils.createThreadFactory("SnapshotTimer"));
      callers = Lists.newArrayList();
      for (int i = 0; i < parallelism; i++) {
        SnapshotCaller caller = new SnapshotCaller();
        snapshotExecutor.execute(caller);
        callers.add(caller);
      }
    }

    JobDataMap jobDataMap = new JobDataMap();
    jobDataMap.put("queue", taskQueue);
    jobDataMap.put("job", job);

    return newJob(JobCaller.class).usingJobData(jobDataMap).build();
  }

  public synchronized void periodSchedule(Period period, SnapshotJob job)
      throws SchedulerException {
    JobDetail jobDetail = createJob(job);

    ScheduleBuilder<SimpleTrigger> builder = null;
    Date start = null;
    switch (period.getTimeUnit()) {
      case SECONDS:
        builder = simpleSchedule().withIntervalInSeconds((int) period.getTime()).repeatForever();
        start = futureDate((int) period.getTime(), IntervalUnit.SECOND);
        break;
      case MINUTES:
        builder = simpleSchedule().withIntervalInMinutes((int) period.getTime()).repeatForever();
        start = futureDate((int) period.getTime(), IntervalUnit.MINUTE);
        break;
      case HOURS:
        builder = simpleSchedule().withIntervalInHours((int) period.getTime()).repeatForever();
        start = futureDate((int) period.getTime(), IntervalUnit.HOUR);
        break;
      case DAYS:
        builder = simpleSchedule()
            .withIntervalInHours((int) TimeUnit.DAYS.toHours(period.getTime())).repeatForever();
        start = futureDate((int) period.getTime(), IntervalUnit.DAY);
        break;
      default:
    }

    if (builder != null) {
      scheduler.scheduleJob(jobDetail, newTrigger().withSchedule(builder).startAt(start).build());
    }
  }

  public synchronized void cronSchedule(String schedulingPattern, SnapshotJob job)
      throws SchedulerException {
    JobDetail jobDetail = createJob(job);
    scheduler.scheduleJob(jobDetail, newTrigger().withSchedule(CronScheduleBuilder
        .cronSchedule(schedulingPattern)).build());
  }

  public void clear() throws SchedulerException {
    if (scheduler != null) {
      scheduler.clear();
    }
  }

  public void stop() throws SchedulerException {
    if (scheduler != null) {
      if (scheduler.isStarted()) {
        scheduler.shutdown();
      }

      snapshotExecutor.shutdownNow();
      LOG.info("Snapshot executor shutdown");
      try {
        long timeout = taskQueue.size() * 100 + TERMINATION_WAIT_TIME;
        if (!snapshotExecutor.awaitTermination(timeout, TimeUnit.MILLISECONDS)) {
          snapshotExecutor.shutdownNow();
          LOG.info("Snapshot executor forced shutdown");
        }
      } catch (InterruptedException e) {
        LOG.error("Failed to shutdown snapshot executor", e);
      }
    }
  }
}
