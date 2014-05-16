/*
 * ┌──┐
 * │  │
 * │Eh│ony
 * └──┘
 */
package org.ehony.camel;

import javax.sql.DataSource;

import static java.lang.Math.max;
import static java.text.MessageFormat.format;

public class TransactSqlExpirableLock extends ExpirableDatabaseLock
{

    /**
     * Query pattern to update record representing required lock which has already
     * expired and belong non-exclusively to another consumer or which belongs to this consumer.
     * <p><b>Important</b> Query may update nothing, meaning lock cannot be obtained.</p>
     * <p>
     * <b>Parameters</b><br/>
     * 0 &ndash; lock identifier<br/>
     * 1 &ndash; consumer identifier<br/>
     * 2 &ndash; expiration timeout<br/>
     * 3 &ndash; lock exclusiveness bit
     * </p>
     */
    public static final String UPDATE_QUERY =
            "update locks"
                    + " set consumer_id = ''{1}'', expires = dateAdd(ms, {2,number,#}, getDate()), exclusive = {3}" 
                    + " where lock_id = ''{0}'' and ((consumer_id <> ''{1}'' and expires <= getDate() and exclusive <> 1) or consumer_id = ''{1}'')";
    
    /**
     * Insert new lock record if one does not yet exist.
     * <p>For parameters see {@link #UPDATE_QUERY} info.</p>
     */
    public static final String INSERT_QUERY =
            "insert into" 
                    + " locks(lock_id, consumer_id, expires, exclusive)" 
                    + " select ''{0}'', ''{1}'', dateAdd(ms, {2,number,#}, getDate()), {3}"
                    + " where not exists (select * from locks where lock_id = ''{0}'')";

    protected TransactSqlExpirableLock(DataSource dataSource, String lockId) {
        super(dataSource, lockId);
    }

    private String getObtainQuery(String template) {
        long timeout = getExpirationTimeout();
        int bit = 0; // Lock is not exclusive, by default.
        if (timeout < 0) {
            bit = 1;
        }
        return format(template, getLockId(), getConsumerId(), max(timeout, 0), bit);
    }

    protected String getUpdateQuery() {
        return getObtainQuery(UPDATE_QUERY);
    }

    protected String getInsertQuery() {
        return getObtainQuery(INSERT_QUERY);
    }
    
    protected String getReleaseQuery() {
        return format(UPDATE_QUERY, getLockId(), getConsumerId(), -1, 0);
    }
}