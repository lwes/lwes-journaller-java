package org.lwes.journaller;

import java.io.IOException;

/**
 * @author fmaritato
 */
public interface JournallerMBean {

    public String getMulticastAddress();
    public int getPort();
    public int getSiteId();
    public int getTtl();

    public long getEventCount();

    public int getCurrentQueueSize();

    public boolean rotate() throws IOException;

    public void shutdown();
}
