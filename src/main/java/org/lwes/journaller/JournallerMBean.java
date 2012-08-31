package org.lwes.journaller;

import java.io.IOException;

/**
 * @author fmaritato
 */
public interface JournallerMBean {

    public String getAddress();
    public int getPort();
    public int getSiteId();
    public int getTtl();

    public long getEventCount();

    public int getCurrentQueueSize();
    public long getDropCount();

    public boolean rotate() throws IOException;

    public void shutdown();
}
