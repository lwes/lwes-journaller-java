package org.lwes.journaller;
/**
 * @author fmaritato
 */

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Ignore;
import org.lwes.Event;
import org.lwes.EventSystemException;
import org.lwes.MapEvent;
import org.lwes.NoSuchAttributeException;
import org.lwes.db.EventTemplateDB;
import org.lwes.listener.DatagramQueueElement;

@Ignore
public class BaseJournallerTest {

    private static transient Log log = LogFactory.getLog(BaseJournallerTest.class);

    public static Calendar last;
    public static Calendar start;
    public static Calendar end;

    static {
        start = Calendar.getInstance();
        start.set(Calendar.YEAR, 2009);
        start.set(Calendar.MONTH, Calendar.OCTOBER);
        start.set(Calendar.DAY_OF_MONTH, 12);
        start.set(Calendar.HOUR_OF_DAY, 15);
        start.set(Calendar.MINUTE, 18);

        last = (Calendar) start.clone();
        last.add(Calendar.MINUTE, -1);

        end = (Calendar) start.clone();
        end.add(Calendar.MINUTE, -1);
    }

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
                                                   InetAddress.getByName("192.168.1.1"),
                                                   9191);
        dqe.setPacket(packet);

        return dqe;
    }

    protected void verifyEvents(MockSequenceDeJournaller mdj)
            throws NoSuchAttributeException, UnknownHostException {

        List<Event> eventList = mdj.getEventList();
        Assert.assertNotNull("Event list was null", eventList);
        Assert.assertEquals("Number of events is wrong", 2, eventList.size());
        for (Event evt : eventList) {
            InetAddress addr = InetAddress.getByAddress(evt.getIPAddress(JournallerConstants.SENDER_IP));
            Assert.assertEquals("192.168.1.1", addr.getHostAddress());
            Assert.assertEquals(9191, (int) evt.getUInt16(JournallerConstants.SENDER_PORT));
            Assert.assertNotNull(evt.getInt64(JournallerConstants.RECEIPT_TIME));
            Assert.assertEquals(256, (int) evt.getInt32("intField1"));
        }
    }
}
