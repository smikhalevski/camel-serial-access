/*
 * ┌──┐
 * │  │
 * │Eh│ony
 * └──┘
 */
package org.ehony.camel;

import org.apache.camel.*;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.spi.RoutePolicy;
import org.apache.commons.dbcp.BasicDataSource;
import org.junit.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;

import static java.lang.Thread.*;
import static org.apache.commons.lang.StringUtils.join;
import static org.junit.Assert.assertEquals;

@Ignore
public class SerialAccessRoutePolicyTest
{


    private static final int THREAD_COUNT = 10,
                             QUEUE_SIZE = 100;
    private static final long POLL_DELAY = 50;
    
    private static final String LOCK_ID = "myLock";
    
    private Queue<Integer> cache = new LinkedList<Integer>(),
                           queue = new LinkedList<Integer>();
    /**
     * Queue contents before running test.
     */
    private String reference;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        cache.clear();
        queue.clear();
        for (int i = 0; i < QUEUE_SIZE; i++) {
            queue.add(i);
        }
        reference = join(queue, ", ");
    }
    
    private Lock createLock() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName("net.sourceforge.jtds.jdbc.Driver");
        dataSource.setUrl("jdbc:jtds:sqlserver://localhost/mydb");
        dataSource.setUsername("sa");
        dataSource.setPassword("password");

        ExpirableDatabaseLock lock = new TransactSqlExpirableLock(dataSource, LOCK_ID);
        lock.setExpirationTimeout(2000);
        return lock;
    }
    
    public RoutePolicy createRoutePolicy() {
        return new SerialAccessRoutePolicy(createLock(), Executors.newFixedThreadPool(1));
    }
    
    @Test
    @SuppressWarnings("unchecked")
    public void testConcurrency() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
        for (int i = 0; i < THREAD_COUNT; i++) {
            pool.submit(new Runnable() {
                @Override
                public void run() {
                    CamelContext context = new DefaultCamelContext();
                    try {
                        context.addRoutes(new RouteBuilder()
                        {
                            @Override
                            public void configure() {
                                from("timer://foo?period=10")
                                        .routeId("camel route for consumer #" + currentThread().getId())
                                        .routePolicy(createRoutePolicy())
                                        .process(new Processor()
                                        {
                                            @Override
                                            public void process(Exchange exchange)
                                                    throws Exception {
                                                if (!queue.isEmpty()) {
                                                    int item = queue.remove();
                                                    cache.add(item);
                                                    // This produces sequential output to console.
                                                    System.out.println(item + " -> thread #" + currentThread().getId());
                                                }
                                            }
                                        });
                            }
                        });
                        context.start();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
        sleep(QUEUE_SIZE * POLL_DELAY * 5);
        assertEquals(reference, join(cache, ", "));
    }
}
