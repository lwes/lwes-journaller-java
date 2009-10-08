package org.lwes.journaller.util;
/**
 * @author fmaritato
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

import java.util.Calendar;
import java.util.List;

public class FormatterTest {

    private static transient Log log = LogFactory.getLog(FormatterTest.class);

    @Test
    public void testFormatReturnList() {
        FilenameFormatter f = new FilenameFormatter();
        List l = f.parse("stuff-%tY%tm%td%tH%tM-%h");
        assertNotNull("List was null", l);
        assertEquals("List size was wrong", 8, l.size());

        Object o = l.get(0);
        assertNotNull("First object was null", o);
        assertTrue("First obj not instance of string", o instanceof String);
        assertEquals("First obj value wrong", "stuff-", o.toString());

        Calendar cal = Calendar.getInstance();
        o = l.get(1);
        assertNotNull("2nd object was null", o);
        assertTrue("2nd obj not instance of string",
                   o instanceof FilenameFormatter.DateTimeObject);
        assertEquals("2nd obj value wrong",
                     ""+cal.get(Calendar.YEAR), o.toString());
    }

    @Test
    public void testLiteralString() {
        FilenameFormatter f = new FilenameFormatter();
        List l = f.parse("stuff");
        assertNotNull("List was null", l);
        assertEquals("List size was wrong", 1, l.size());
    }
}
