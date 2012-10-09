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

public class Rotate extends MapEvent {
    private static transient Log log = LogFactory.getLog(Rotate.class);

    // Constants used as attribute keys
    public static final String NAME = "Journaller::Rotate";

    public static final String TS = "ts";
    public static final String NUM = "num";
    public static final String FILENAME = "filename";

    public Rotate() throws EventSystemException {
        super(NAME, null);
    }

    public Rotate(long timestamp, long numEvents, String filename)
            throws EventSystemException {
        super(NAME, null);
        setTimestamp(timestamp);
        setNum(numEvents);
        setFilename(filename);
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

    public void setFilename(String filename) throws EventSystemException {
        setString(FILENAME, filename);
    }
    public String getFilename() throws NoSuchAttributeException {
        return getString(FILENAME);
    }

    public String toString() {
        StringBuilder buf = new StringBuilder(256);
        try {
            buf.append("Rotate {ts=").append(getTimestamp())
                    .append("; num=").append(getNum())
                    .append("; filename=").append(getFilename())
                    .append("}");
        }
        catch (NoSuchAttributeException e) {
            log.error(e.getMessage(), e);
        }
        return buf.toString();
    }
}
