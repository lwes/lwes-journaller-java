package org.lwes;
/**
 * User: fmaritato
 * Date: Apr 14, 2009
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lwes.listener.EventHandler;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.zip.GZIPOutputStream;

/**
 * Write events to GzipOuputStream. When we receive a Command::Rotate event, rotate
 * the log file.
 */
public class GZIPEventHandler implements EventHandler {
    private transient Log log = LogFactory.getLog(GZIPEventHandler.class);

    private Object semaphore = new Object();

    private String fileName;
    private String generatedFileName;
    private GZIPOutputStream out;

    /**
     * Create the Event handler and open the output stream to the file.
     *
     * @param file File we should be writing to.
     * @throws IOException if there is a problem opening the file for writing.
     */
    public GZIPEventHandler(String file) throws IOException {
        this.fileName = file;

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
        Calendar c = Calendar.getInstance();
        String fn = fileName +
                    c.get(Calendar.YEAR) +
                    pad(c.get(Calendar.MONTH), 2) +
                    pad(c.get(Calendar.DAY_OF_MONTH), 2) +
                    pad(c.get(Calendar.HOUR_OF_DAY), 2) + "00";
        File f = new File(fn + ".gz");
        for (int i = 1; f.exists(); i++) {
            f = new File(fn + "-" + i + ".gz");
        }
        setGeneratedFileName(f.getAbsolutePath());
        return f;
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

    /**
     * The header is <size of message><time><senderIp><senderport><siteId>
     *
     * @param event Event we are writing the header for.
     * @param buf   preallocated ByteBuffer for writing the header bytes into.
     */
    private void writeHeader(Event event, ByteBuffer buf) {
        byte[] data = event.serialize();
        int size = data.length;
        log.debug("size: " + size);
        long time = System.currentTimeMillis();

        try {
            InetAddress sender = event.getInetAddress("SenderIP");
            int port = event.getUInt16("SenderPort");
            int siteId = 0;
            try {
                siteId = event.getUInt16("SiteID");
            }
            catch (AttributeNotSetException e) {
                // who cares
            }

            buf.putInt(size)
                    .putLong(time)
                    .put(sender.getAddress())
                    .putInt(port)
                    .putInt(siteId);

        }
        catch (Exception e) {
            log.error(e.getMessage(), e);
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
            log.debug("Received event: " + event);
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
                    writeHeader(event, b);
                    out.write(b.array(), 0, DeJournaller.MAX_HEADER_SIZE);
                    byte[] data = event.serialize();
                    //b.put(data);
                    // Even though we allocated size for the max length, only write to the stream
                    // the actual length.
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
        log.info("** Caught interrupt signal. Shutting down...");
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

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getGeneratedFileName() {
        return generatedFileName;
    }

    public void setGeneratedFileName(String generatedFileName) {
        this.generatedFileName = generatedFileName;
    }
}
