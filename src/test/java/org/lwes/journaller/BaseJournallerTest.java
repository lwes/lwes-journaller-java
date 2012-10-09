package org.lwes.journaller;
/**
 * @author fmaritato
 */

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Ignore;
import org.lwes.Event;
import org.lwes.EventSystemException;
import org.lwes.MapEvent;
import org.lwes.db.EventTemplateDB;
import org.lwes.listener.DatagramQueueElement;

@Ignore
public class BaseJournallerTest {

    private static transient Log log = LogFactory.getLog(BaseJournallerTest.class);

    public Event createRotateEvent()
            throws EventSystemException,
                   UnknownHostException {

        EventTemplateDB evtDb = new EventTemplateDB();
        evtDb.initialize();
        Event evt = new MapEvent("Command::Rotate", false, evtDb);
        evt.setIPAddress("SenderIP", InetAddress.getByName("192.168.1.1"));
        evt.setUInt16("SenderPort", 9191);
        evt.setInt64("ReceiptTime", System.currentTimeMillis());
        evt.setUInt16("SiteID", 0);
        return evt;
    }
                       
    public DatagramQueueElement createTestEvent()
            throws EventSystemException,
                   UnknownHostException {

        long ts = System.currentTimeMillis();
        EventTemplateDB evtDb = new EventTemplateDB();
        evtDb.initialize();
        Event evt = new MapEvent("TestEvent", false, evtDb);
        evt.setString("field1", "testing");
        evt.setInt32("intField1", 256);

        DatagramQueueElement dqe = new DatagramQueueElement();
        dqe.setTimestamp(ts);
        byte[] buffer = evt.serialize();
        DatagramPacket packet = new DatagramPacket(buffer,
                                                   buffer.length,
                                                   InetAddress.getLocalHost(),
                                                   9191);
        dqe.setPacket(packet);

        return dqe;
    }
}
