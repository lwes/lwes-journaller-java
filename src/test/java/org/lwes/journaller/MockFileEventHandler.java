package org.lwes.journaller;
/**
 * @author fmaritato
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lwes.Event;
import org.lwes.journaller.handler.AbstractFileEventHandler;
import org.lwes.listener.DatagramQueueElement;

import java.io.IOException;
import java.util.Calendar;

public class MockFileEventHandler extends AbstractFileEventHandler {

    private static transient Log log = LogFactory.getLog(MockFileEventHandler.class);
    private String pattern;
    private Calendar calendar;

    public MockFileEventHandler(String pattern) {
        this.pattern = pattern;
        setFilenamePattern(pattern);
        generateFilename();
    }

    public MockFileEventHandler(String pattern, Calendar c) {
        this.calendar = c;
        this.pattern = pattern;
        setFilenamePattern(pattern);
        generateFilename(c);
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

    protected void rotate() throws IOException {
        generateFilename(calendar);
    }

    public void destroy() {
    }

    public void handleEvent(Event event) {
    }

    public void handleEvent(DatagramQueueElement element) throws IOException {
    }
}
