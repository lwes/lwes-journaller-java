package org.lwes.journaller; /**
 * User: fmaritato
 * Date: Apr 22, 2009
 */

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lwes.Event;
import org.lwes.EventSystemException;
import org.lwes.db.EventTemplateDB;
import org.lwes.journaller.handler.GZIPEventHandler;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

public class GZIPEventHandlerTest extends TestCase {
    private transient Log log = LogFactory.getLog(GZIPEventHandlerTest.class);

    /**
     * Manually inject 10 events into the GZIP event handler, force a rotation,
     * verify the number of events in the first file, throw 10 more events, shutdown
     * the handler then verify the events in the rotated file.
     */
    public void testHandler() {
        try {
            GZIPEventHandler handler = new GZIPEventHandler("target/junit");
            String generatedFile1 = handler.getGeneratedFilename();
            for (int i = 0; i < 10; i++) {
                handler.handleEvent(createTestEvent());
            }
            handler.handleEvent(createRotateEvent());
            MockDeJournaller mdj = new MockDeJournaller();
            mdj.setFileName(generatedFile1);
            mdj.setGzipped(true);
            mdj.run();
            List<Event> eventList = mdj.getEventList();
            assertNotNull("Event list was null", eventList);
            assertEquals("Number of events is wrong", 10, eventList.size());

            String generatedFile2 = handler.getGeneratedFilename();
            for (int i = 0; i < 10; i++) {
                handler.handleEvent(createTestEvent());
            }
            handler.destroy();
            mdj = new MockDeJournaller();
            mdj.setFileName(generatedFile2);
            mdj.setGzipped(true);
            mdj.run();
            eventList = mdj.getEventList();
            assertNotNull("Event list was null", eventList);
            assertEquals("Number of events is wrong", 10, eventList.size());
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
        evt.setIPAddress("SenderIP", InetAddress.getByName("192.168.1.1"));
        evt.setUInt16("SenderPort", 9191);
        evt.setInt64("ReceiptTime", System.currentTimeMillis());
        evt.setUInt16("SiteID", 0);
        return evt;
    }

    private Event createTestEvent()
            throws EventSystemException,
                   UnknownHostException {

        EventTemplateDB evtDb = new EventTemplateDB();
        evtDb.initialize();
        Event evt = new Event("TestEvent", false, evtDb);
        evt.setIPAddress("SenderIP", InetAddress.getByName("192.168.1.1"));
        evt.setUInt16("SenderPort", 9191);
        evt.setInt64("ReceiptTime", System.currentTimeMillis());
        evt.setUInt16("SiteID", 0);
        evt.setString("field1", "testing");
        evt.setInt32("intField1", 256);
        return evt;
    }
}
