package org.lwes.journaller;
/**
 * I had to call this BaseFileEventHandlerTest because surefire skips it if it has Abstract
 * in the name... :(
 * 
 * @author fmaritato
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.lwes.NoSuchAttributeException;
import org.lwes.NoSuchAttributeTypeException;
import org.lwes.NoSuchEventException;
import org.lwes.journaller.handler.NIOEventHandler;

import java.net.UnknownHostException;

public class BaseFileEventHandlerTest extends BaseJournallerTest {

    private static transient Log log = LogFactory.getLog(BaseFileEventHandlerTest.class);

    @Test
    public void testIsRotateEvent()
            throws UnknownHostException,
                   NoSuchAttributeException,
                   NoSuchAttributeTypeException,
                   NoSuchEventException {

        NIOEventHandler handler = new NIOEventHandler();
        assertTrue(handler.isRotateEvent(createRotateEvent().serialize()));
        assertFalse(handler.isRotateEvent(createTestEvent().serialize()));
    }

}
