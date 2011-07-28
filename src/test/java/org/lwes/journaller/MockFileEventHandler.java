package org.lwes.journaller;
/**
 * @author fmaritato
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lwes.journaller.handler.AbstractFileEventHandler;
import org.lwes.listener.DatagramQueueElement;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.IOException;
import java.util.Calendar;

public class MockFileEventHandler extends AbstractFileEventHandler {

    private static transient Log log = LogFactory.getLog(MockFileEventHandler.class);
    private String pattern;
    private Calendar calendar;

    public MockFileEventHandler(String pattern) {
        this(pattern, null);
    }

    public MockFileEventHandler(String pattern, Calendar c) {
        this.calendar = c;
        this.pattern = pattern;
        setFilenamePattern(pattern);
        setFilename(generateFilename(c));
    }

    public String getFileExtension() {
        return null;
    }

    public Calendar getCalendar() {
        return calendar;
    }

    public void setCalendar(Calendar calendar) {
        this.calendar = calendar;
    }

    public boolean rotate() throws IOException {
        setFilename(generateFilename(calendar));

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
