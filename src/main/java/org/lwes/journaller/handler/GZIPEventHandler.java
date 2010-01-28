package org.lwes.journaller.handler;
/**
 * User: fmaritato
 * Date: Apr 14, 2009
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lwes.Event;
import org.lwes.EventSystemException;
import org.lwes.journaller.DeJournaller;
import org.lwes.journaller.event.Rotate;
import org.lwes.journaller.util.EventHandlerUtil;
import org.lwes.listener.DatagramQueueElement;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.zip.GZIPOutputStream;

/**
 * Write events to GzipOuputStream. When we receive a Command::Rotate event, rotate
 * the log file.
 */
public class GZIPEventHandler extends AbstractFileEventHandler {

    private transient Log log = LogFactory.getLog(GZIPEventHandler.class);

    private final Object semaphore = new Object();
    private GZIPOutputStream out;

    /**
     * Create the Event handler and open the output stream to the file.
     *
     * @param filePattern Filename pattern to use when creating the file
     *                    we should be writing to.
     * @throws IOException if there is a problem opening the file for writing.
     */
    public GZIPEventHandler(String filePattern) throws IOException {
        setFilenamePattern(filePattern);
        generateFilename();
        createFileHandle();
    }

    /**
     * Convenience method to open a GZIPOutputStream.
     *
     * @throws IOException if there is a problem opening a handle to the file.
     */
    protected void createFileHandle() throws IOException {
        File newFile = new File(getFilename());
        if (newFile.exists()) {
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
        }
        out = new GZIPOutputStream(new FileOutputStream(getFilename(), true));
        if (log.isDebugEnabled()) {
            log.debug("Created a new log file: " + getFilename());
        }
    }

    /**
     * Rotate the output file when called. It will name the new file based on the
     * server time. If a file already exists for that hour it will append "-1" or
     * increasing number at the end.
     *
     * @throws IOException if there is a problem opening the file.
     */
    protected void rotate() throws IOException {
        String oldfile = getFilename();
        if (out != null) {
            out.close();
        }
        generateFilename();
        createFileHandle();
        try {
            emit(new Rotate(System.currentTimeMillis(), getNumEvents(), oldfile));
            setNumEvents(0);
        }
        catch (EventSystemException e) {
            log.error(e.getMessage(), e);
        }
    }

    public String getFileExtension() {
        return ".gz";
    }

    /**
     * This version of handleEvent is the one actually called by the journaller.
     *
     * @param element DatagramQueueElement containing the serialized bytes of the event.
     * @throws IOException
     */
    public void handleEvent(DatagramQueueElement element) throws IOException {
        synchronized (semaphore) {
            DatagramPacket packet = element.getPacket();
            long ts = System.currentTimeMillis();
            emitHealth();
            // If we get a rotate event, it is from the local host and it
            // isn't too soon since the last one then rotate the log.
            if (isRotateEvent(packet.getData()) &&
                !tooSoonToRotate(ts) &&
                "/127.0.0.1".equals(packet.getAddress().toString())) {
                lastRotateTimestamp = ts;
                rotate();
            }
            else if (!isJournallerEvent(packet.getData())) {
                incrNumEvents();
                ByteBuffer b = ByteBuffer.allocate(DeJournaller.MAX_HEADER_SIZE);
                EventHandlerUtil.writeHeader(packet.getLength(),
                                             element.getTimestamp(),
                                             packet.getAddress(),
                                             packet.getPort(),
                                             getSiteId(),
                                             b);
                out.write(b.array(), 0, DeJournaller.MAX_HEADER_SIZE);
                out.write(packet.getData());
                out.flush();
            }
        }
    }

    /**
     * The meat of this class, called for every event captured. If the event is
     * "Command::Rotate" then rotate the output file before writing any further events.
     * Otherwise, serialize the header and event and write them to the file.
     *
     * @param event Event to write to the file.
     */
    public void handleEvent(Event event) {
        synchronized (semaphore) {
            if ("Command::Rotate".equals(event.getEventName()) &&
                !tooSoonToRotate(System.currentTimeMillis())) {
                try {
                    rotate();
                }
                catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
            }
            else {
                try {
                    ByteBuffer b = ByteBuffer.allocate(DeJournaller.MAX_HEADER_SIZE);
                    EventHandlerUtil.writeHeader(event, b);
                    out.write(b.array(), 0, DeJournaller.MAX_HEADER_SIZE);
                    byte[] data = event.serialize();
                    out.write(data);
                    out.flush();
                }
                catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    /**
     * Hook to make sure the output stream gets closed.
     */
    public void destroy() {
        if (log.isInfoEnabled()) {
            log.info("** Caught interrupt signal. Shutting down...");
        }
        synchronized (semaphore) {
            try {
                out.flush();
                out.close();
            }
            catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

}
