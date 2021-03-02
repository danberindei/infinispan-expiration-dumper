package com.redhat.datagrid;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import org.infinispan.Cache;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskExecutionMode;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
public class ExpirationDumper implements ServerTask<String> {

   private Cache<?, ?> cache;

   @Override
   public void setTaskContext(TaskContext taskContext) {
      cache = taskContext.getCache().get();
   }

   @Override
   public String call() throws Exception {
      Logger logger = Logger.getLogger("EXPIRATIONS");
      logger.info("EXPIRATION STATS DUMP");
      DataContainer<?, ?> dc = cache.getAdvancedCache().getDataContainer();
      Map<Long, AtomicInteger> lifespanDistribution = initHistogram();
      Map<Long, AtomicInteger> maxIdleDistribution = initHistogram();
      Iterator<? extends InternalCacheEntry<?, ?>> it1 = dc.iteratorIncludingExpired();
      while (it1.hasNext()) {
         InternalCacheEntry<?, ?> entry = it1.next();
         long ls = entry.canExpire() ? entry.getLifespan() / 1000 : 0;
         updateHistogram(ls, lifespanDistribution);
         long mi = entry.canExpire() ? entry.getMaxIdle() / 1000 : 0;
         updateHistogram(mi, maxIdleDistribution);
      }
      StringWriter writer = new StringWriter();
      writer.write(cache.getAdvancedCache().getRpcManager().getAddress() + ":\n");
      printHistogram(writer, "Lifespan", lifespanDistribution);
      printHistogram(writer, "MaxIdle", maxIdleDistribution);
      return writer.toString();
   }

   private Map<Long, AtomicInteger> initHistogram() {
      Map<Long, AtomicInteger> map = new HashMap<>();
      map.put(0L
            , new AtomicInteger());
      return map;
   }

   void updateHistogram(long value, Map<Long, AtomicInteger> distribution) {
      AtomicInteger count = distribution.get(value);
      if (count == null) {
         count = new AtomicInteger(1);
         distribution.put(value, count);
      } else {
         count.incrementAndGet();
      }
   }

   void printHistogram(StringWriter writer, String kind, Map<Long, AtomicInteger> distribution) {
      for (Map.Entry<Long, AtomicInteger> next : distribution.entrySet()) {
         writer.write(kind + " = " + next.getKey() + "s Count = " + next.getValue() + "\n");
      }
   }

   @Override
   public TaskExecutionMode getExecutionMode() {
      return TaskExecutionMode.ALL_NODES;
   }

   @Override
   public String getName() {
      return "ExpirationDumper";
   }
}
