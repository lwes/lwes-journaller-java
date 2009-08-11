package org.lwes.journaller.handler;
/**
 * User: fmaritato
 * Date: Apr 14, 2009
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lwes.Event;
import org.lwes.journaller.DeJournaller;
import org.lwes.journaller.util.EventHandlerUtil;
import org.lwes.listener.DatagramQueueElement;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.nio.ByteBuffer;
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
     * @param file File we should be writing to.
     * @throws IOException if there is a problem opening the file for writing.
     */
    public GZIPEventHandler(String file) throws IOException {
        setFilename(file);
        createFileHandle(getFile());
    }

    /**
     * Convenience method to open a GZIPOutputStream.
     *
     * @param f File object pointing to the event log we shall be writing to
     * @throws IOException if there is a problem opening a handle to the file.
     */
    protected void createFileHandle(File f) throws IOException {
        log.debug("Using file: " + f.getAbsolutePath());
        out = new GZIPOutputStream(new FileOutputStream(f));
    }

    /**
     * Rotate the output file when called. It will name the new file based on the
     * server time. If a file already exists for that hour it will append "-1" or
     * increasing number at the end.
     *
     * @throws IOException if there is a problem opening the file.
     */
    protected void rotate() throws IOException {
        if (out != null) {
            out.close();
        }
        createFileHandle(getFile());
    }

    /**
     * Returns a File object pointing to where the logs should be written. Takes care of naming
     * conventions.
     *
     * @return File
     */
    protected File getFile() {
        String fn = getDateString();
        File f = new File(fn + ".gz");
        for (int i = 1; f.exists(); i++) {
            f = new File(fn + "-" + i + ".gz");
        }
        setGeneratedFilename(f.getAbsolutePath());
        return f;
    }

    public void handleEvent(DatagramQueueElement element) throws IOException {
        synchronized (semaphore) {
            DatagramPacket packet = element.getPacket();
            if (isRotateEvent(packet.getData())) {
                rotate();
            }
            else {
                ByteBuffer b = ByteBuffer.allocate(DeJournaller.MAX_HEADER_SIZE);
                EventHandlerUtil.writeHeader(packet.getLength(),
                                         element.getTimestamp(),
                                         packet.getAddress(),
                                         packet.getPort(),
                                         0, // TODO
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
            if (log.isDebugEnabled()) {
                log.debug("Received event: " + event);
            }
            if ("Command::Rotate".equals(event.getEventName())) {
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
