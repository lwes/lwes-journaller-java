package org.lwes;
/**
 * User: fmaritato
 * Date: Apr 21, 2009
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import junit.framework.TestCase;

import java.util.List;
import java.util.LinkedList;

public class DeJournalTest extends TestCase {
    private transient Log log = LogFactory.getLog(DeJournalTest.class);

    public void testDejournal() {
        try {
            MockDeJournaller m = new MockDeJournaller();
            m.setFileName("src/test/resources/test.gz");
            m.run();

            List<Event> eventList = m.getEventList();
            assertNotNull("EventList was null", eventList);
            assertEquals("Wrong number of events parsed", 100, eventList.size());            
        }
        catch (Exception e) {
            log.error(e.getMessage(), e);
            fail(e.getMessage());
        }
    }

    class MockDeJournaller extends DeJournaller {
        private List<Event> eventList = new LinkedList<Event>();

        @Override
        public void handleEvent(Event event) {
            eventList.add(event);
        }

        public List<Event> getEventList() {
            return eventList;
        }

        public void setEventList(List<Event> eventList) {
            this.eventList = eventList;
        }
    }
}
