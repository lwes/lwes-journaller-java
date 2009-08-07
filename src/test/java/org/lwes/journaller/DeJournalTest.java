package org.lwes.journaller;
/**
 * User: fmaritato
 * Date: Apr 21, 2009
 */

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lwes.Event;

import java.util.List;

public class DeJournalTest extends TestCase {
    private transient Log log = LogFactory.getLog(DeJournalTest.class);

    public void testDejournal() {
        try {
            MockDeJournaller m = new MockDeJournaller();
            m.setFileName("src/test/resources/test.gz");
            m.setGzipped(true);
            m.run();

            List<Event> eventList = m.getEventList();
            assertNotNull("EventList was null", eventList);
            assertEquals("Wrong number of events parsed", 100, eventList.size());
            for (Event e : eventList) {
                assertEquals("Event name was wrong", "MyEvent", e.getEventName());
                assertNotNull("SenderIP was null", e.get("SenderIP"));
                assertNotNull("SenderPort was null", e.get("SenderPort"));
            }
        }
        catch (Exception e) {
            log.error(e.getMessage(), e);
            fail(e.getMessage());
        }
    }
}
