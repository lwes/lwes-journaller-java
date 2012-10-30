package org.lwes.journaller;
/**
 * @author fmaritato
 */

import java.io.IOException;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lwes.journaller.handler.AbstractFileEventHandler;
import org.lwes.listener.DatagramQueueElement;

public class MockFileEventHandler extends AbstractFileEventHandler {

    private static transient Log log = LogFactory.getLog(MockFileEventHandler.class);

    public MockFileEventHandler() {
    }

    public String getFileExtension() {
        return ".mock";
    }

    public boolean rotate() throws IOException {
        return true;
    }

    public void destroy() {
    }

    public void handleEvent(DatagramQueueElement element) throws IOException {
    }

    @Override
    public void swapOutputStream() {

    }

    @Override
    public void closeOutputStream() throws IOException {
        // no op
    }

    @Override
    public void createOutputStream(String filename) throws IOException {
        // no op
    }

    public ObjectName getObjectName() throws MalformedObjectNameException {
        return null;
    }
}
