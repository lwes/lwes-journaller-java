package org.lwes.journaller;
/**
 * I had to call this BaseFileEventHandlerTest because surefire skips it if it has Abstract
 * in the name... :(
 *
 * @author fmaritato
 */

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.lwes.EventSystemException;
import org.lwes.journaller.handler.NIOEventHandler;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BaseFileEventHandlerTest extends BaseJournallerTest {

    private static transient Log log = LogFactory.getLog(BaseFileEventHandlerTest.class);

    @Test
    public void testIsRotateEvent()
            throws IOException,
                   EventSystemException {

        NIOEventHandler handler = new NIOEventHandler("target/nio-junit", "%tQ");
        assertTrue(handler.isRotateEvent(createRotateEvent().serialize()));
        assertFalse(handler.isRotateEvent(createTestEvent().getPacket().getData()));
    }

}
