/*
 * ┌──┐
 * │  │
 * │Eh│ony
 * └──┘
 */
package org.ehony.camel;

import javax.sql.DataSource;
import java.sql.*;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.*;

import static java.lang.Thread.*;
import static org.apache.commons.lang.Validate.*;

public abstract class DatabaseLock implements Lock
{
    
    private DataSource dataSource;
    private String lockId, consumerId = UUID.randomUUID().toString();
    private long delay = 1000;
    protected int tryCount = 3;

    protected DatabaseLock(DataSource dataSource, String lockId) {
        notNull(dataSource, "Data source expected.");
        notEmpty(lockId, "Non-empty lock identifier expected.");
        this.dataSource = dataSource;
        this.lockId = lockId;
    }

    protected abstract String getUpdateQuery();
    
    protected abstract String getInsertQuery();
    
    protected abstract String getReleaseQuery();
    
    /**
     * Pauses this thread for given amount of msec.
     */
    private static void pause(long delay) {
        try {
            sleep(delay);
        } catch (InterruptedException ie) {
            currentThread().interrupt();
        }
    }
    
    /**
     * Get lock identifier.
     */
    public String getLockId() {
        return lockId;
    }

    /**
     * Get unique identifier used to distinguish lock consumers.
     */
    public String getConsumerId() {
        return consumerId;
    }


    /**
     * Get number of additional lock obtaining retries.
     */
    public int getRetryCount() {
        return tryCount;
    }

    /**
     * Set number of additional lock obtaining retries.
     * <p>Zero means that consumer would try to obtain lock only once.
     * This increases possibility of race conditions during inserting
     * and updating the lock row.</p>
     * 
     * @param count number of retries.
     */
    public void setRetryCount(int count) {
        isTrue(count >= 0, "Expected non-negative retry count.");
        this.tryCount = count;
    }

    public long getRetryDelay() {
        return delay;
    }

    public void setRetryDelay(long delay) {
        isTrue(delay > 0, "Positive delay expected.");
        this.delay = delay;
    }
    
    private int update(String query) throws SQLException {
        Connection c = dataSource.getConnection();
        PreparedStatement stmt = null;
        int rows = 0;
        try {
            stmt = c.prepareStatement(query);
            rows = stmt.executeUpdate();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } finally {
                c.close();
            }
        }
        return rows;
    }

    /**
     * Tries to quietly obtain lock.
     */
    private boolean obtainLock() {
        try {
            return update(getUpdateQuery()) > 0 || update(getInsertQuery()) > 0; 
        }catch (SQLException e) {
            throw new LockException("Cannot obtain lock " + lockId + " by " + consumerId, e);
        }
    }

    @Override
    public void lock() {
        while (!obtainLock()) {
            pause(delay);
        }
    }

    @Override
    public void unlock() {
        for (int i = 0; i < tryCount + 1; i++) {
            try {
                if (update(getReleaseQuery()) == 0) {
                    throw new IllegalMonitorStateException();
                }
                return;
            } catch (SQLException e) {
                throw new LockException("Cannot unlock " + lockId + " by " + consumerId, e);
            }
        }
    }

    @Override
    public boolean tryLock() {
        int i = 0;
        while (!obtainLock()) {
            if (i++ >= tryCount) {
                return false;
            }
            pause(delay);
        }
        return true;
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        while (!obtainLock()) {
            if (currentThread().isInterrupted()) {
                throw new InterruptedException();
            }
            pause(delay);
        }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        long timeout = unit.toMillis(time);
        long ts = System.currentTimeMillis();
        while (!obtainLock()) {
            if (currentThread().isInterrupted()) {
                throw new InterruptedException();
            }
            if (System.currentTimeMillis() - ts >= timeout) {
                return false;
            }
            pause(delay);
        }
        return true;
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }
}
