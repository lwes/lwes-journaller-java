package org.lwes.journaller;
/**
 * @author fmaritato
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.File;
import java.util.Calendar;

public class FileEventHandlerTest {

    private static transient Log log = LogFactory.getLog(FileEventHandlerTest.class);

    @Test
    public void testRotateFileName() throws IOException {
        Calendar c = Calendar.getInstance();
        c.set(Calendar.YEAR, 2009);
        c.set(Calendar.MONTH, Calendar.OCTOBER);
        c.set(Calendar.DAY_OF_MONTH, 12);
        c.set(Calendar.HOUR_OF_DAY, 15);
        c.set(Calendar.MINUTE, 18);
        MockFileEventHandler m = new MockFileEventHandler("junit-file-%tY%tm%td%tH%tM", c);
        assertEquals("junit-file-200910121518", m.getFilename());
        File f = new File(m.getFilename());
        if (f.exists()) {
            f.delete();
        }
        c.set(Calendar.MINUTE, 19);
        m.setCalendar(c);
        m.rotate();
        assertEquals("junit-file-200910121519", m.getFilename());
        f = new File(m.getFilename());
        if (f.exists()) {
            f.delete();
        }
    }

    @Test
    public void testRotateFileName2() throws IOException, InterruptedException {

        MockFileEventHandler m = new MockFileEventHandler("junit-file-%tY%tm%td%tH%tM");
        log.debug(m.getFilename());
        File f = new File(m.getFilename());
        if (f.exists()) {
            f.delete();
        }
        Thread.sleep(1000);
        m.rotate();
        m.getFilename();
        f = new File(m.getFilename());
        if (f.exists()) {
            f.delete();
        }
        log.debug(m.getFilename());
    }
}
