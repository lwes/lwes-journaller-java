package org.lwes.journaller.handler;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.IOException;

/**
 * @author fmaritato
 */
public interface EventHandlerMBean {

    public String getFilename();

    public long getEventCount();

    public int getSiteId();

    public int getHealthInterval();

    public boolean rotate() throws IOException;

    public ObjectName getObjectName() throws MalformedObjectNameException;
}
