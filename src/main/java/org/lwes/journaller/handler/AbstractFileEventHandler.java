package org.lwes.journaller.handler;
/**
 * @author fmaritato
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lwes.Event;
import org.lwes.EventSystemException;
import org.lwes.journaller.event.Health;
import org.lwes.journaller.event.Rotate;
import org.lwes.journaller.util.FilenameFormatter;
import org.lwes.listener.DatagramQueueElement;

import java.io.File;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.DatagramSocket;
import java.util.Calendar;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractFileEventHandler implements DatagramQueueElementHandler {

    private static transient Log log = LogFactory.getLog(AbstractFileEventHandler.class);

    private FilenameFormatter formatter = new FilenameFormatter();

    private String filename;
    private String filenamePattern;

    private int siteId = 0;
    private long rotateGracePeriod = 1000 * 30; // 30 seconds
    protected long lastRotateTimestamp = 0;
    private static final byte[] ROTATE = "Command::Rotate".getBytes();
    private static final byte[] JOURNALLER = "Journaller::".getBytes();

    private DatagramSocket socket;
    private InetAddress address;
    private int port;

    private AtomicLong eventCount = new AtomicLong();

    private int healthInterval = 60;
    private long lastHealthTime = System.currentTimeMillis();

    protected final Object lock = new Object();

    /**
     * This method checks if the filename we want to use already exists. If it does
     * we move it to a different name to avoid clobbering data we may want to keep.
     *
     * @param newFile the proposed new file object.
     */
    public void moveExistingFile(File newFile) {
        if (newFile.exists()) {
            if (log.isDebugEnabled()) {
                log.debug(newFile.getAbsolutePath() + " exists. Renaming");
            }
            Calendar c = Calendar.getInstance();
            // TODO I don't like this...
            StringBuilder buf = new StringBuilder()
                .append(c.get(Calendar.YEAR)).append(c.get(Calendar.MONTH))
                .append(c.get(Calendar.DAY_OF_MONTH)).append(c.get(Calendar.HOUR_OF_DAY))
                .append(c.get(Calendar.MINUTE))
                .append(getFilename());
            boolean succeeded = newFile.renameTo(new File(buf.toString()));
            if (!succeeded) {
                log.error("File rename failed. " + newFile.getAbsolutePath());
            }
            else {
                if (log.isDebugEnabled()) {
                    log.debug("Renamed file to: " + newFile.getAbsolutePath());
                }
            }
        }
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

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
        if (log.isDebugEnabled()) {
            log.debug("Generated a new filename: " + fn);
        }
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
            return false;
        }
    }

    public void emitHealth() {
        long now = System.currentTimeMillis();
        if (healthInterval > 0 &&
            (now - lastHealthTime) > (healthInterval * 1000)) {
            try {
                emit(new Health(now, getEventCount(), healthInterval));
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
                                                      getAddress(),
                                                      getPort());
                socket.send(p);
            }
        }
        catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * Hook to make sure the output stream gets closed.
     */
    public void destroy() {
        if (log.isInfoEnabled()) {
            log.info("Closing output stream...");
        }
        try {
            synchronized (lock) {
                swapOutputStream();
            }
            closeOutputStream();
        }
        catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * Rotate the output file when called. It will name the new file based on the
     * server time. If a file already exists for that hour it will append "-1" or
     * increasing number at the end.
     *
     * @return false if we did NOT rotate the file, true if we did
     * @throws java.io.IOException if there is a problem opening the file.
     */
    public boolean rotate() throws IOException {
        long ts = System.currentTimeMillis();
        if (tooSoonToRotate(ts)) {
            if (log.isDebugEnabled()) {
                log.debug("Too soon to rotate.");
            }
            return false;
        }

        String oldfile = getFilename();
        if (log.isDebugEnabled()) {
            log.debug("oldfile: " + oldfile);
        }

        String newFilename = generateFilename();
        createOutputStream(newFilename);

        // Sync on the close and reopen of the file and counting the number of events.
        synchronized (lock) {
            swapOutputStream();
            this.filename = newFilename;
            lastRotateTimestamp = ts;
            try {
                emit(new Rotate(System.currentTimeMillis(), getEventCount(), oldfile));
                setEventCount(0);
            }
            catch (EventSystemException e) {
                log.error(e.getMessage(), e);
            }
        }

        closeOutputStream();

        return true;
    }

    public void handleEvent(DatagramQueueElement element) throws IOException {
        emitHealth();
    }

    public abstract String getFileExtension();

    public abstract void swapOutputStream();

    public abstract void createOutputStream(String filename) throws IOException;

    public abstract void closeOutputStream() throws IOException;

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public InetAddress getAddress() {
        return address;
    }

    public void setAddress(InetAddress address) {
        this.address = address;
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

    public DatagramSocket getSocket() {
        return socket;
    }

    public void setSocket(DatagramSocket socket) {
        this.socket = socket;
    }

    public long getEventCount() {
        return eventCount.get();
    }

    public void setEventCount(long eventCount) {
        this.eventCount.set(eventCount);
    }

    public void incrNumEvents() {
        eventCount.incrementAndGet();
    }

    public int getHealthInterval() {
        return healthInterval;
    }

    public void setHealthInterval(int healthInterval) {
        this.healthInterval = healthInterval;
    }
}
