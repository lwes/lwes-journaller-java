package org.lwes.journaller.handler;
/**
 * @author fmaritato
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lwes.listener.EventHandler;

import java.io.IOException;
import java.util.Calendar;

public abstract class AbstractFileEventHandler implements EventHandler {

    private static transient Log log = LogFactory.getLog(AbstractFileEventHandler.class);

    private String filename;
    private String generatedFilename;

    protected String getDateString() {
        Calendar c = Calendar.getInstance();
        StringBuilder fn = new StringBuilder()
                .append(getFilename())
                .append(c.get(Calendar.YEAR))
                .append(pad(c.get(Calendar.MONTH), 2))
                .append(pad(c.get(Calendar.DAY_OF_MONTH), 2))
                .append(pad(c.get(Calendar.HOUR_OF_DAY), 2))
                .append(pad(c.get(Calendar.MINUTE), 2));
        return fn.toString();
    }

    /**
     * Make sure number is the specified number of digits. If not <b>prepend</b>
     * 0's. I use this for dates: 1-9 come back as 01-09.
     *
     * @param number the value we want to verify
     * @param digits the number of digits number should be.
     * @return String containing the padded number.
     */
    protected String pad(int number, int digits) {
        String str = "" + number;
        if (str.length() < digits) {
            for (int i = 0; i < digits - str.length(); i++) {
                str = "0" + str;
            }
        }
        return str;
    }

    protected abstract void rotate() throws IOException;

    public String getGeneratedFilename() {
        return generatedFilename;
    }

    public void setGeneratedFilename(String generatedFilename) {
        this.generatedFilename = generatedFilename;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }


}
