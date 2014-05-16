/*
 * ┌──┐
 * │  │
 * │Eh│ony
 * └──┘
 */
package org.ehony.camel;

import org.junit.*;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;

import static java.lang.Thread.*;
import static org.apache.commons.lang.StringUtils.join;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class DatabaseLockTest
{

    private static final int THREAD_COUNT = 10,
                             QUEUE_SIZE = 100;
    private static final long POLL_DELAY = 50;
    
    private static final String UPDATE_QUERY = "update",
                                INSERT_QUERY = "insert",
                                RELEASE_QUERY = "release",
                                LOCK_ID = "myLock";
    
    private Queue<Integer> cache = new LinkedList<Integer>(),
                           queue = new LinkedList<Integer>();
    /**
     * Queue contents before running test.
     */
    private String reference;
    
    // Following fields represent database record for the lock.
    private volatile boolean canUpdate, canInsert;
    private volatile String ownerId;

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
        
        // At first we have no record about lock in database,
        // so we can only insert a new one.
        canUpdate = false;
        canInsert = true;
        ownerId = null;
    }
    
    private DataSource createDataSource(final String consumerId) {
        try {
            PreparedStatement updateStatement = mock(PreparedStatement.class);
            when(updateStatement.executeUpdate()).thenAnswer(new Answer<Integer>() {
                @Override
                public Integer answer(InvocationOnMock invocation) throws Throwable {
                    // We can lock record only if it belongs to nobody or its is ours.
                    if (canUpdate && (ownerId == null || ownerId.equals(consumerId))) {
                        ownerId = consumerId;
                        return 1;
                    }
                    return 0;
                }
            });
            
            PreparedStatement insertStatement = mock(PreparedStatement.class);
            when(insertStatement.executeUpdate()).thenAnswer(new Answer<Integer>() {
                @Override
                public Integer answer(InvocationOnMock invocation)throws Throwable {
                    if (canInsert) {
                        // We inserted a new record and locked our lock.
                        // New lock with the same name cannot be inserted.
                        canUpdate = true; // Record now exists, so we can update it.
                        canInsert = false;
                        ownerId = consumerId; // Lock is obtained right after insert.
                        return 1;
                    }
                    return 0;
                }
            });
            
            PreparedStatement releaseStatement = mock(PreparedStatement.class);
            when(releaseStatement.executeUpdate()).thenAnswer(new Answer<Integer>() {
                @Override
                public Integer answer(InvocationOnMock invocation) throws Throwable {
                    // We can unlock record only is we have locked it.
                    if (consumerId.equals(ownerId)) {
                        ownerId = null;
                        return 1;
                    }
                    return 0;
                }
            });
            
            Connection connection = mock(Connection.class);
            when(connection.prepareStatement(eq(UPDATE_QUERY))).thenReturn(updateStatement);
            when(connection.prepareStatement(eq(INSERT_QUERY))).thenReturn(insertStatement);
            when(connection.prepareStatement(eq(RELEASE_QUERY))).thenReturn(releaseStatement);
            
            DataSource dataSource = mock(DataSource.class);
            when(dataSource.getConnection()).thenReturn(connection);
            
            return dataSource;
        } catch (Exception e) {
            throw new RuntimeException("Data source mocking failed.", e);
        }
    }
    
    private Lock createLock(final String consumerId) {
        return new DatabaseLock(createDataSource(consumerId), LOCK_ID) {

            @Override
            public String getConsumerId() {
                return consumerId;
            }

            @Override
            protected String getUpdateQuery() {
                return UPDATE_QUERY;
            }

            @Override
            protected String getInsertQuery() {
                return INSERT_QUERY;
            }

            @Override
            protected String getReleaseQuery() {
                return RELEASE_QUERY;
            }
        };
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testConcurrency() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
        for (int i = 0; i < THREAD_COUNT; i++) {
            pool.submit(new Runnable() {
                @Override
                public void run() {
                    // Pretend each runnable in isolated environment
                    // and each has its own distributed lock instance.
                    Lock lock = createLock("consumer #" + currentThread().getId());
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
        sleep(QUEUE_SIZE * POLL_DELAY);
        // TODO Check array to string conversion.
        // assertEquals(reference, join(cache, ", "));
    }
}
