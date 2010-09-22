package org.lwes.journaller;

/**
 * User: fmaritato
 * Date: Apr 22, 2009
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.lwes.Event;
import org.lwes.EventSystemException;
import org.lwes.journaller.handler.NIOEventHandler;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class NIOEventHandlerTest extends BaseJournallerTest {
    private transient Log log = LogFactory.getLog(NIOEventHandlerTest.class);

    @Test
    public void testDatagramPacket()
            throws IOException, EventSystemException {

        NIOEventHandler handler = new NIOEventHandler("target/junit-nio-%tH%tM%tS");
        String generatedFile1 = handler.getFilename();
        for (int i = 0; i < 10; i++) {
            handler.handleEvent(createTestEvent());
        }

        MockDeJournaller mdj = new MockDeJournaller();
        mdj.setFileName(generatedFile1);
        mdj.run();
        List<Event> eventList = mdj.getEventList();
        assertNotNull("Event list was null", eventList);
        if (log.isDebugEnabled()) {
            for (Event e : eventList) {
                log.debug(e);
            }
        }
        assertEquals("Number of events is wrong", 10, eventList.size());
        File f = new File(generatedFile1);
        if (f.exists()) {
            f.delete();
        }
    }

    /**
     * Manually inject 10 events into the GZIP event handler, force a rotation,
     * verify the number of events in the first file, throw 10 more events, shutdown
     * the handler then verify the events in the rotated file.
     */
    @Test
    public void testHandler() throws IOException, EventSystemException {

        NIOEventHandler handler = new NIOEventHandler("target/junit-nio-%tH%tM%tS");
        String generatedFile1 = handler.getFilename();
        for (int i = 0; i < 10; i++) {
            handler.handleEvent(createTestEvent());
        }
        try {
            Thread.sleep(1000);
        }
        catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }
        handler.rotate();

        MockDeJournaller mdj = new MockDeJournaller();
        mdj.setFileName(generatedFile1);
        mdj.run();
        List<Event> eventList = mdj.getEventList();
        assertNotNull("Event list was null", eventList);
        if (log.isDebugEnabled()) {
            for (Event e : eventList) {
                log.debug(e);
            }
        }
        assertEquals("Number of events is wrong", 10, eventList.size());
        File f = new File(generatedFile1);
        if (f.exists()) {
            assertTrue(f.delete());
        }

        String generatedFile2 = handler.getFilename();
        for (int i = 0; i < 10; i++) {
            handler.handleEvent(createTestEvent());
        }
        handler.destroy();
        mdj = new MockDeJournaller();
        mdj.setFileName(generatedFile2);
        mdj.run();
        eventList = mdj.getEventList();
        assertNotNull("Event list was null", eventList);
        assertEquals("Number of events is wrong", 10, eventList.size());
        f = new File(generatedFile2);
        if (f.exists()) {
            assertTrue(f.delete());
        }
    }

}