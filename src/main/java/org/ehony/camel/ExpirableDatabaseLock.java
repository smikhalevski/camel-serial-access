/*
 * ┌──┐
 * │  │
 * │Eh│ony
 * └──┘
 */
package org.ehony.camel;

import javax.sql.DataSource;

import static java.lang.Math.max;
import static org.apache.commons.lang.Validate.isTrue;

public abstract class ExpirableDatabaseLock extends DatabaseLock
{
    
    private long timeout = 60000;

    protected ExpirableDatabaseLock(DataSource dataSource, String lockId) {
        super(dataSource, lockId);
    }

    /**
     * Get lock expiration timeout exploited by this consumer.
     * @return For infinite timeout <code>-1</code> is returned, otherwise positive amount of msec.
     */
    public long getExpirationTimeout() {
        return timeout;
    }

    /**
     * Set database lock lease timeout for this consumer.
     * <p>Providing negative value causes infinite timeout. Consumer would obtain
     * an exclusive lock access in this case, so unlocking can be done only on
     * explicit {@link #unlock()} method invocation.</p>
     * <p><b>Important</b> Be aware of stopping the world if database connection
     * cannot be established to perform unlocking.</p>
     * 
     * @param timeout lock lease timeout.
     */
    public void setExpirationTimeout(long timeout) {
        isTrue(timeout != 0, "Zero timeout prohibited.");
        this.timeout = max(-1, timeout);
    }
}
