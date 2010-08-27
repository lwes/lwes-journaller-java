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
import org.lwes.db.EventTemplateDB;
import org.lwes.journaller.handler.SequenceFileHandler;
import org.lwes.listener.DatagramQueueElement;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class SequenceFileHandlerTest extends TestCase {

    private transient Log log = LogFactory.getLog(SequenceFileHandlerTest.class);
     
    /**
     * Manually inject 10 events into the GZIP event handler, force a rotation,
     * verify the number of events in the first file, throw 10 more events, shutdown
     * the handler then verify the events in the rotated file.
     */
    public void testHandler() {
        try {
            //SequenceFileHandler handler = new SequenceFileHandler("target/junit-seq-%tY%tm%td%tH%tM%tS");
            SequenceFileHandler handler = new SequenceFileHandler("target/junit-seq");
            String generatedFile1 = handler.getFilename();
            if (log.isDebugEnabled()) {
                log.debug("generated file: " + generatedFile1);
            }
            handler.handleEvent(createTestEvent());
            handler.destroy();
        }
        catch (Exception e) {
            log.error(e.getMessage(), e);
            fail(e.getMessage());
        }
    }

    private Event createRotateEvent()
        throws EventSystemException,
               UnknownHostException {

        EventTemplateDB evtDb = new EventTemplateDB();
        evtDb.initialize();
        Event evt = new Event("Command::Rotate", false, evtDb);
        return evt;
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
