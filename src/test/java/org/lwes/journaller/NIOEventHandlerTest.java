package org.lwes.journaller;

/**
 * User: fmaritato
 * Date: Apr 22, 2009
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.junit.Test;
import org.lwes.Event;
import org.lwes.NoSuchAttributeException;
import org.lwes.NoSuchAttributeTypeException;
import org.lwes.NoSuchEventException;
import org.lwes.journaller.handler.NIOEventHandler;
import org.lwes.journaller.util.EventHandlerUtil;
import org.lwes.listener.DatagramQueueElement;

import java.io.IOException;
import java.net.DatagramPacket;
import java.nio.ByteBuffer;
import java.util.List;

public class NIOEventHandlerTest extends BaseJournallerTest {
    private transient Log log = LogFactory.getLog(NIOEventHandlerTest.class);

    public void testDatagramPacket()
            throws IOException, NoSuchAttributeException,
                   NoSuchAttributeTypeException, NoSuchEventException {

        NIOEventHandler handler = new NIOEventHandler("target/junit");
        String generatedFile1 = handler.getGeneratedFilename();
        for (int i = 0; i < 10; i++) {
            Event evt = createTestEvent();
            byte[] b1 = evt.serialize();
            byte[] bytes = new byte[b1.length+JournallerConstants.MAX_HEADER_SIZE];
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            EventHandlerUtil.writeEvent(evt, buf);
            DatagramPacket p = new DatagramPacket(bytes, bytes.length);
            p.setData(bytes);
            DatagramQueueElement element = new DatagramQueueElement();
            element.setPacket(p);
            element.setTimestamp(System.currentTimeMillis());
            handler.handleEvent(element);
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

    }

    /**
     * Manually inject 10 events into the GZIP event handler, force a rotation,
     * verify the number of events in the first file, throw 10 more events, shutdown
     * the handler then verify the events in the rotated file.
     */
    @Test
    public void testHandler()
            throws IOException, NoSuchAttributeException,
                   NoSuchAttributeTypeException, NoSuchEventException {

        NIOEventHandler handler = new NIOEventHandler("target/junit");
        String generatedFile1 = handler.getGeneratedFilename();
        for (int i = 0; i < 10; i++) {
            handler.handleEvent(createTestEvent());
        }
        handler.handleEvent(createRotateEvent());

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

        String generatedFile2 = handler.getGeneratedFilename();
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

    }

}