package org.lwes.journaller.handler;

import org.lwes.listener.DatagramQueueElement;

import java.io.IOException;

/**
 * @author fmaritato
 */
public interface DatagramQueueElementHandler {

    void handleEvent(DatagramQueueElement element) throws IOException;
    void destroy();
    
}
