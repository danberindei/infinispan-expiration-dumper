package com.redhat.datagrid;

import java.util.Collections;
import java.util.List;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.remoting.RemoteException;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
public class TaskExecutor {
   public static void main(String[] args) {
      if (args.length != 4) {
         System.err.println(TaskExecutor.class.getName() + " host port cachename taskname");
         System.exit(0);
      }
      String host = args[0];
      String port = args[1];
      String cacheName = args[2];
      String taskName = args[3];
      System.out.printf("Running task %s on cache %s on server %s:%s", taskName, cacheName, host, port);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServer().host(host).port(Integer.parseInt(port)).version(ProtocolVersion.PROTOCOL_VERSION_26);
      RemoteCacheManager rcm = new RemoteCacheManager(builder.build());
      RemoteCache<Object, Object> cache = rcm.getCache(cacheName);
      try {
         List<?> result = cache.execute(taskName, Collections.emptyMap());
         System.out.println("Done.");
         for (Object o : result) {
            System.out.println(o);
         }
      } catch (HotRodClientException e) {
         System.err.println("Please make sure the task is deployed on all the servers");
         System.err.printf("Got an error from %s:%s. %s", host, port, e);
      }
   }
}
