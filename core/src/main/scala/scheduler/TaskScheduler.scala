package org.example.spark.scheduler

import java.util.concurrent.{Executors, TimeUnit}

object TaskScheduler {
  def scheduleTask(task: Runnable, initialDelay: Long, period: Long, unit: TimeUnit): Unit = {
    val scheduler = Executors.newScheduledThreadPool(1)
    scheduler.scheduleAtFixedRate(task, initialDelay, period, unit)

    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      scheduler.shutdown()
      if (!scheduler.awaitTermination(1, TimeUnit.MINUTES)) {
        scheduler.shutdownNow()
      }
    }))
  }
}