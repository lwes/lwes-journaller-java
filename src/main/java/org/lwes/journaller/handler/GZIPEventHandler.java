package org.lwes.journaller.handler;

/**
 * User: fmaritato
 * Date: 9/22/2010
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lwes.journaller.JournallerConstants;
import org.lwes.journaller.util.EventHandlerUtil;
import org.lwes.listener.DatagramQueueElement;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.nio.ByteBuffer;
import java.util.zip.GZIPOutputStream;

/**
 * Write events to GzipOuputStream.
 */
public class GZIPEventHandler extends AbstractFileEventHandler {

    private transient Log log = LogFactory.getLog(GZIPEventHandler.class);

    private GZIPOutputStream out = null;
    private GZIPOutputStream tmp = null;
    private GZIPOutputStream old = null;

    public GZIPEventHandler() {
    }

    /**
     * Create the Event handler and open the output stream to the file.
     *
     * @param filePattern Filename pattern to use when creating the file
     *                    we should be writing to.
     * @throws IOException if there is a problem opening the file for writing.
     */
    public GZIPEventHandler(String filePattern) throws IOException {
        setFilenamePattern(filePattern);
        String fn = generateFilename();
        createOutputStream(fn);
        swapOutputStream();
        setFilename(fn);
    }

    /**
     * Convenience method to open a GZIPOutputStream.
     *
     * @throws IOException if there is a problem opening a handle to the file.
     */
    public void createOutputStream(String filename) throws IOException {
        File newFile = new File(filename);
        moveExistingFile(newFile);
        tmp = new GZIPOutputStream(new FileOutputStream(filename, true));
        if (log.isDebugEnabled()) {
            log.debug("Created a new log file: " + getFilename());
        }
    }

    /**
     * @return The gzip file extension: ".gz"
     */
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
        DatagramPacket packet = element.getPacket();
        if (!isJournallerEvent(packet.getData())) {
            ByteBuffer b = ByteBuffer.allocate(JournallerConstants.MAX_HEADER_SIZE);
            EventHandlerUtil.writeHeader(packet.getLength(),
                                         element.getTimestamp(),
                                         packet.getAddress(),
                                         packet.getPort(),
                                         getSiteId(),
                                         b);
            synchronized (lock) {
                if (out != null) {
                    incrNumEvents();
                    out.write(b.array(), 0, JournallerConstants.MAX_HEADER_SIZE);
                    out.write(packet.getData());
                    out.flush();
                }
            }
        }
    }

    public void swapOutputStream() {
        old = out;
        out = tmp;
        tmp = null;
    }

    public void closeOutputStream() throws IOException {
        if (old != null) {
            old.flush();
            old.close();
            old = null;
        }
    }

    public ObjectName getObjectName() throws MalformedObjectNameException {
        return new ObjectName("org.lwes:name=GZIPEventHandler");
    }
}
