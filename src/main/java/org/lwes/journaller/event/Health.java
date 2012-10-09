package org.lwes.journaller.event;
/**
 * @author fmaritato
 */

import java.math.BigInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lwes.EventSystemException;
import org.lwes.MapEvent;
import org.lwes.NoSuchAttributeException;

public class Health extends MapEvent {

    private static transient Log log = LogFactory.getLog(Health.class);

    // Constants used as attribute keys
    public static final String NAME = "Journaller::Health";

    public static final String TS = "ts";
    public static final String NUM = "num";
    public static final String INTERVAL = "interval";

    public Health() throws EventSystemException {
        super(NAME, null);
    }

    public Health(long timestamp, long numEvents, int interval)
            throws EventSystemException {
        super(NAME, null);
        setTimestamp(timestamp);
        setNum(numEvents);
        setInterval(interval);
    }


    public void setTimestamp(long millis) throws EventSystemException {
        setUInt64(TS, millis);
    }

    public BigInteger getTimestamp() throws NoSuchAttributeException {
        return getUInt64(TS);
    }

    public void setNum(long numEvents) throws EventSystemException {
        setUInt64(NUM, numEvents);
    }

    public BigInteger getNum() throws NoSuchAttributeException {
        return getUInt64(NUM);
    }

    public void setInterval(int seconds) throws EventSystemException {
        setInt32(INTERVAL, seconds);
    }

    public int getInterval() throws NoSuchAttributeException {
        return getInt32(INTERVAL);
    }

    public String toString() {
        StringBuilder buf = new StringBuilder(256);
        try {
            buf.append("Health {ts=").append(getTimestamp())
                    .append("; num=").append(getNum())
                    .append("; interval=").append(getInterval())
                    .append("}");
        }
        catch (NoSuchAttributeException e) {
            log.error(e.getMessage(), e);
        }
        return buf.toString();
    }
}
