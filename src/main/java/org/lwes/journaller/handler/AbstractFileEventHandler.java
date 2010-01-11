package org.lwes.journaller.handler;
/**
 * @author fmaritato
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lwes.Event;
import org.lwes.EventSystemException;
import org.lwes.journaller.event.Health;
import org.lwes.journaller.util.FilenameFormatter;
import org.lwes.listener.EventHandler;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.Calendar;

public abstract class AbstractFileEventHandler implements EventHandler, DatagramQueueElementHandler {

    private static transient Log log = LogFactory.getLog(AbstractFileEventHandler.class);

    private FilenameFormatter formatter = new FilenameFormatter();

    private String filename;
    private String filenamePattern;

    private int siteId = 0;
    private long rotateGracePeriod = 1000 * 30; // 30 seconds
    private long lastRotateTimestamp = 0;
    private static final byte[] ROTATE = "Command::Rotate".getBytes();
    private static final byte[] JOURNALLER = "Journaller::".getBytes();

    private MulticastSocket socket;
    private InetAddress multicastAddr;
    private int multicastPort;

    private long numEvents = 0;

    private int healthInterval = 60;
    private long lastHealthTime = System.currentTimeMillis();

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

    public String generateFilename() {
        return generateFilename(null);
    }

    public String generateFilename(Calendar c) {
        String fn = formatter.format(filenamePattern, c);
        if (getFileExtension() != null &&
            !fn.endsWith(getFileExtension())) {
            fn += getFileExtension();
        }
        this.filename = fn;

        return fn;
    }

    public boolean isJournallerEvent(byte[] bytes) {
        for (int i = 0; i < JOURNALLER.length; i++) {
            if (bytes[i + 1] != JOURNALLER[i]) {
                return false;
            }
        }
        return true;
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

    public void emitHealth() {
        long now = System.currentTimeMillis();
        if (healthInterval > 0 &&
            (now - lastHealthTime) > (healthInterval * 1000)) {
            try {
                emit(new Health(now, getNumEvents(), healthInterval));
            }
            catch (EventSystemException e) {
                log.error(e.getMessage(), e);
            }
            lastHealthTime = now;
        }
    }

    public void emit(Event evt) {
        try {
            if (log.isInfoEnabled()) {
                log.info(evt);
            }
            if (socket != null) {
                byte[] bytes = evt.serialize();
                DatagramPacket p = new DatagramPacket(bytes,
                                                      bytes.length,
                                                      getMulticastAddr(),
                                                      getMulticastPort());
                socket.send(p);
            }
        }
        catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    public int getMulticastPort() {
        return multicastPort;
    }

    public void setMulticastPort(int multicastPort) {
        this.multicastPort = multicastPort;
    }

    public InetAddress getMulticastAddr() {
        return multicastAddr;
    }

    public void setMulticastAddr(InetAddress multicastAddr) {
        this.multicastAddr = multicastAddr;
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

    public MulticastSocket getSocket() {
        return socket;
    }

    public void setSocket(MulticastSocket socket) {
        this.socket = socket;
    }

    public long getNumEvents() {
        return numEvents;
    }

    public void setNumEvents(long numEvents) {
        this.numEvents = numEvents;
    }

    public void incrNumEvents() {
        numEvents++;
    }

    public int getHealthInterval() {
        return healthInterval;
    }

    public void setHealthInterval(int healthInterval) {
        this.healthInterval = healthInterval;
    }
}
