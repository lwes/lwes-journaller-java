package org.lwes.journaller;
/**
 * @author fmaritato
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Ignore;
import org.lwes.Event;
import org.lwes.NoSuchAttributeException;
import org.lwes.NoSuchAttributeTypeException;
import org.lwes.NoSuchEventException;
import org.lwes.db.EventTemplateDB;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Ignore
public class BaseJournallerTest {

    private static transient Log log = LogFactory.getLog(BaseJournallerTest.class);

    public Event createRotateEvent()
            throws NoSuchAttributeException,
                   NoSuchAttributeTypeException,
                   NoSuchEventException,
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

    public Event createTestEvent()
            throws NoSuchAttributeException,
                   NoSuchAttributeTypeException,
                   NoSuchEventException,
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
