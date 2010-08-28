package org.lwes.journaller;
/**
 * User: fmaritato
 * Date: Apr 22, 2009
 */

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lwes.Event;
import org.lwes.EventSystemException;
import org.lwes.NoSuchAttributeException;
import org.lwes.db.EventTemplateDB;
import org.lwes.journaller.handler.SequenceFileHandler;
import org.lwes.listener.DatagramQueueElement;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

public class SequenceFileHandlerTest extends TestCase {

    private transient Log log = LogFactory.getLog(SequenceFileHandlerTest.class);

    /**
     * "Emit" some events, make sure they end up in the file. Verify they can be read
     * and contain the fields that the journaller should be inserting.
     */
    public void testHandler() throws IOException, EventSystemException {
        SequenceFileHandler handler = new SequenceFileHandler("target/junit-seq1-%tY%tm%td%tH%tM%tS%tL");
        String generatedFile1 = handler.getFilename();
        if (log.isDebugEnabled()) {
            log.debug("generated file: " + generatedFile1);
        }
        handler.handleEvent(createTestEvent());
        handler.handleEvent(createTestEvent());
        handler.destroy();

        MockSequenceDeJournaller mdj = new MockSequenceDeJournaller();
        mdj.setFileName(generatedFile1);
        mdj.run();

        verifyEvents(mdj);
    }

    public void testRotation() throws IOException, EventSystemException, InterruptedException {
        SequenceFileHandler handler = new SequenceFileHandler("target/junit-seq2-%tY%tm%td%tH%tM%tS%tL");
        String generatedFile1 = handler.getFilename();
        if (log.isDebugEnabled()) {
            log.debug("generated file: " + generatedFile1);
        }
        handler.handleEvent(createTestEvent());
        handler.handleEvent(createTestEvent());
        Thread.sleep(1000);
        handler.handleEvent(createRotateEvent());

        MockSequenceDeJournaller mdj = new MockSequenceDeJournaller();
        mdj.setFileName(generatedFile1);
        mdj.run();

        verifyEvents(mdj);
        String generatedFile2 = handler.getFilename();
        if (log.isDebugEnabled()) {
            log.debug("generated file: " + generatedFile2);
        }
        handler.handleEvent(createTestEvent());
        handler.handleEvent(createTestEvent());
        handler.destroy();

        mdj.clear();
        mdj.setFileName(generatedFile2);
        mdj.run();

        verifyEvents(mdj);
    }

    private void verifyEvents(MockSequenceDeJournaller mdj)
        throws NoSuchAttributeException, UnknownHostException {

        List<Event> eventList = mdj.getEventList();
        assertNotNull("Event list was null", eventList);
        assertEquals("Number of events is wrong", 2, eventList.size());
        for (Event evt : eventList) {
            InetAddress addr = InetAddress.getByAddress(evt.getIPAddress(JournallerConstants.SENDER_IP));
            assertEquals("192.168.1.1", addr.getHostAddress());
            assertEquals(9191, (int) evt.getUInt16(JournallerConstants.SENDER_PORT));
            assertNotNull(evt.getInt64(JournallerConstants.RECEIPT_TIME));
            assertEquals(256, (int) evt.getInt32("intField1"));
        }
    }

    private DatagramQueueElement createRotateEvent()
        throws EventSystemException,
               UnknownHostException {

        EventTemplateDB evtDb = new EventTemplateDB();
        evtDb.initialize();
        Event evt = new Event("Command::Rotate", false, evtDb);

        DatagramQueueElement dqe = new DatagramQueueElement();
        byte[] data = evt.serialize();
        dqe.setPacket(new DatagramPacket(data, data.length, InetAddress.getByName("127.0.0.1"), 9191));
        dqe.setTimestamp(System.currentTimeMillis());

        return dqe;
    }

    private DatagramQueueElement createTestEvent()
        throws EventSystemException,
               UnknownHostException {

        EventTemplateDB evtDb = new EventTemplateDB();
        evtDb.initialize();
        Event evt = new Event("TestEvent", false, evtDb);
        evt.setString("field1", "testing");
        evt.setInt32("intField1", 256);

        DatagramQueueElement dqe = new DatagramQueueElement();
        byte[] data = evt.serialize();
        dqe.setPacket(new DatagramPacket(data, data.length, InetAddress.getByName("192.168.1.1"), 9191));
        dqe.setTimestamp(System.currentTimeMillis());

        return dqe;
    }
}
