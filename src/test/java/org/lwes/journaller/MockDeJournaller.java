package org.lwes.journaller;
/**
 * User: fmaritato
 * Date: Apr 22, 2009
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lwes.Event;

import java.util.LinkedList;
import java.util.List;

public class MockDeJournaller extends DeJournaller {
    private transient Log log = LogFactory.getLog(MockDeJournaller.class);

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
