package org.lwes.journaller;
/**
 * @author fmaritato
 */

import java.io.IOException;
import java.util.Calendar;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FileEventHandlerTest {

    private static transient Log log = LogFactory.getLog(FileEventHandlerTest.class);

    @Test
    public void testRotateFileName() throws IOException {
        Calendar s = Calendar.getInstance();
        s.set(Calendar.YEAR, 2009);
        s.set(Calendar.MONTH, Calendar.OCTOBER);
        s.set(Calendar.DAY_OF_MONTH, 12);
        s.set(Calendar.HOUR_OF_DAY, 15);
        s.set(Calendar.MINUTE, 18);

        Calendar e = (Calendar) s.clone();
        e.add(Calendar.MINUTE, 1);

        MockFileEventHandler m = new MockFileEventHandler();
        m.setFilename("all_events.log");
        m.setFilenamePattern("%tY%tm%td%tH%tM");

        assertEquals("all_events.log.200910121518.200910121519.mock", m.generateRotatedFilename(s, e));
    }
}
