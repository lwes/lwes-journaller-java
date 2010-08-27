package org.lwes.journaller.handler;
/**
 * @author fmaritato
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.lwes.Event;
import org.lwes.EventSystemException;
import org.lwes.db.EventTemplateDB;
import org.lwes.journaller.JournallerConstants;
import org.lwes.journaller.event.Rotate;
import org.lwes.listener.DatagramQueueElement;

import java.io.IOException;
import java.net.DatagramPacket;

public class SequenceFileHandler extends AbstractFileEventHandler implements JournallerConstants {

    private static transient Log log = LogFactory.getLog(SequenceFileHandler.class);

    private EventTemplateDB eventTemplate = new EventTemplateDB();
    private final Object semaphore = new Object();
    private SequenceFile.Writer out = null;
    private BytesWritable key = new BytesWritable();

    public SequenceFileHandler(String filePattern) throws IOException {
        setFilenamePattern(filePattern);
        generateFilename();
        createFileHandle();
    }

    private void createFileHandle() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(getFilename());
        out = SequenceFile.createWriter(fs, conf, path,
                                        BytesWritable.class,
                                        NullWritable.class,
                                        SequenceFile.CompressionType.BLOCK);
    }

    @Override
    public String getFileExtension() {
        return ".seq";
    }

    @Override
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

    public void handleEvent(DatagramQueueElement element) throws IOException {
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
            Event event = null;
            try {
                event = new Event(packet.getData(), false, eventTemplate);

                event.setIPAddress(JournallerConstants.SENDER_IP, packet.getAddress());
                event.setUInt16(JournallerConstants.SENDER_PORT, packet.getPort());
                event.setUInt16(JournallerConstants.SITE_ID, getSiteId());
                event.setInt64(JournallerConstants.RECEIPT_TIME, System.currentTimeMillis());

                byte[] bytes = event.serialize();
                key.set(bytes, 0, bytes.length);
                out.append(key, NullWritable.get());
            }
            catch (EventSystemException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public void destroy() {
        if (out != null) {
            try {
                out.close();
            }
            catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }
    }
}
