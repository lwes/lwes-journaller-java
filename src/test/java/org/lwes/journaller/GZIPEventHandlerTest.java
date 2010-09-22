package org.lwes.journaller;
/**
 * User: fmaritato
 * Date: Apr 22, 2009
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.lwes.Event;
import org.lwes.journaller.handler.GZIPEventHandler;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GZIPEventHandlerTest extends BaseJournallerTest {
    private transient Log log = LogFactory.getLog(GZIPEventHandlerTest.class);

    /**
     * Manually inject 10 events into the GZIP event handler, force a rotation,
     * verify the number of events in the first file, throw 10 more events, shutdown
     * the handler then verify the events in the rotated file.
     */
    @Test
    public void testHandler() {
        try {
            GZIPEventHandler handler = new GZIPEventHandler("target/junit-gzip-%tY%tm%td%tH%tM%tS");
            String generatedFile1 = handler.getFilename();
            if (log.isDebugEnabled()) {
                log.debug("generated file: "+generatedFile1);
            }
            for (int i = 0; i < 10; i++) {
                handler.handleEvent(createTestEvent());
            }
            Thread.sleep(1000);
            handler.rotate();

            MockDeJournaller mdj = new MockDeJournaller();
            mdj.setFileName(generatedFile1);
            mdj.setGzipped(true);
            mdj.run();
            List<Event> eventList = mdj.getEventList();
            assertNotNull("Event list was null", eventList);
            assertEquals("Number of events is wrong", 10, eventList.size());

            String generatedFile2 = handler.getFilename();
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

            File f = new File(generatedFile1);
            if (f.exists()) {
                assertTrue(f.delete());
            }
            f = new File(generatedFile2);
            if (f.exists()) {
                assertTrue(f.delete());
            }
        }
        catch (Exception e) {
            log.error(e.getMessage(), e);
            fail(e.getMessage());
        }
    }

}
