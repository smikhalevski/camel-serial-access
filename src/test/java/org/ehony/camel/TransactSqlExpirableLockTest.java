/*
 * ┌──┐
 * │  │
 * │Eh│ony
 * └──┘
 */
package org.ehony.camel;

import org.apache.commons.dbcp.BasicDataSource;
import org.junit.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;

import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;
import static org.apache.commons.lang.StringUtils.join;
import static org.junit.Assert.assertEquals;

@Ignore
public class TransactSqlExpirableLockTest
{

    private static final int THREAD_COUNT = 10,
                             QUEUE_SIZE = 100;
    private static final long POLL_DELAY = 100;
    
    private static final String LOCK_ID = "testLock";
    
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
        
        // Join concatenates all items from queue, ex. join([1,2,3]) = "1, 2, 3"
        reference = join(queue, ", ");
    }

    private Lock createLock() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName("net.sourceforge.jtds.jdbc.Driver");
        dataSource.setUrl("jdbc:jtds:sqlserver://localhost/mydb");
        dataSource.setUsername("sa");
        dataSource.setPassword("password");
        
        return new TransactSqlExpirableLock(dataSource, LOCK_ID);
    }
    
    @Test
    @SuppressWarnings("unchecked")
    public void testConcurrency() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
        for (int i = 0; i < THREAD_COUNT; i++) {
            pool.submit(new Runnable() {
                @Override
                public void run() {
                    Lock lock = createLock();
                    while (true) {
                        if (lock.tryLock()) {
                            try {
                                if (queue.isEmpty()) {
                                    break;
                                } else {
                                    int item = queue.remove();
                                    cache.add(item);
                                    // This produces sequential output to console.
                                    System.out.println(item + " -> thread #" + currentThread().getId());
                                }
                            } finally {
                                lock.unlock();
                            }
                        }
                        try {
                            sleep(POLL_DELAY);
                        } catch (InterruptedException e) {
                            throw new RuntimeException("Unexpected interruption.", e);
                        }
                    }
                }
            });
        }
        sleep(QUEUE_SIZE * POLL_DELAY * 2);
        assertEquals(reference, join(cache, ", "));
    }
}
