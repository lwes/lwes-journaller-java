package org.lwes.journaller.handler;
/**
 * @author fmaritato
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lwes.journaller.util.FilenameFormatter;
import org.lwes.listener.EventHandler;

import java.io.IOException;

public abstract class AbstractFileEventHandler implements EventHandler, DatagramQueueElementHandler {

    private static transient Log log = LogFactory.getLog(AbstractFileEventHandler.class);

    private FilenameFormatter formatter = new FilenameFormatter();

    private String filename;
    private String filenamePattern;

    private int siteId = 0;
    private long rotateGracePeriod = 1000 * 30; // 30 seconds
    private long lastRotateTimestamp = 0;
    private static final byte[] ROTATE = "Command::Rotate".getBytes();

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

    public abstract String getFileExtension();

    public String getFilename() {
        return filename;
    }

    public String getFilename(boolean reset) {
        if (reset) {
            setFilename(filenamePattern);
        }
        return getFilename();
    }

    public void setFilename(String filenamePattern) {
        this.filenamePattern = filenamePattern;
        String fn = formatter.format(filenamePattern);
        if (getFileExtension() != null &&
            !fn.endsWith(getFileExtension())) {
            fn += getFileExtension();
        }
        this.filename = fn;
    }

    public boolean isRotateEvent(byte[] bytes) {
        for (int i = 0; i < ROTATE.length; i++) {
            if (bytes[i + 1] != ROTATE[i]) {
                return false;
            }
        }
        return true;
    }

    public boolean tooSoonToRotate(long time) {
        if (lastRotateTimestamp > 0 &&
            (time - lastRotateTimestamp) < rotateGracePeriod) {
            if (log.isDebugEnabled()) {
                log.debug("Too soon to rotate!");
            }
            return true;
        }
        else {
            lastRotateTimestamp = time;
            return false;
        }
    }

    public long getRotateGracePeriod() {
        return rotateGracePeriod;
    }

    public void setRotateGracePeriod(long rotateGracePeriod) {
        this.rotateGracePeriod = rotateGracePeriod;
    }

    public int getSiteId() {
        return siteId;
    }

    public void setSiteId(int siteId) {
        this.siteId = siteId;
    }

    public String getFilenamePattern() {
        return filenamePattern;
    }

    public void setFilenamePattern(String filenamePattern) {
        this.filenamePattern = filenamePattern;
    }
}
