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
import org.lwes.listener.DatagramQueueElement;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.IOException;
import java.net.DatagramPacket;

public class SequenceFileHandler extends AbstractFileEventHandler implements JournallerConstants {

    private static transient Log log = LogFactory.getLog(SequenceFileHandler.class);

    private EventTemplateDB eventTemplate = new EventTemplateDB();
    private SequenceFile.Writer out = null;
    private BytesWritable key = new BytesWritable();
    private final Object lock = new Object();

    public SequenceFileHandler(String filePattern) throws IOException {
        setFilenamePattern(filePattern);
        generateFilename();
        createOutputStream();
    }

    @Override
    public void closeAndReopen() throws IOException {
        synchronized (lock) {
            closeOutputStream();
            generateFilename();
            createOutputStream();
        }
    }

    public void createOutputStream() throws IOException {
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

    public void handleEvent(DatagramQueueElement element) throws IOException {
        DatagramPacket packet = element.getPacket();
        emitHealth();
        if (!isJournallerEvent(packet.getData())) {
            incrNumEvents();
            Event event = null;
            try {
                // TODO: maybe make the key the header, and the value the event?
                // That way we don't need to serialize into an Event here.
                event = new Event(packet.getData(), false, eventTemplate);
                if (!event.containsKey("enc")) {
                    event.setInt16(Event.ENCODING, Event.DEFAULT_ENCODING);
                }
                event.setIPAddress(JournallerConstants.SENDER_IP, packet.getAddress());
                event.setUInt16(JournallerConstants.SENDER_PORT, packet.getPort());
                event.setUInt16(JournallerConstants.SITE_ID, getSiteId());
                event.setInt64(JournallerConstants.RECEIPT_TIME, System.currentTimeMillis());

                byte[] bytes = event.serialize();
                key.set(bytes, 0, bytes.length);
                synchronized (lock) {
                    out.append(key, NullWritable.get());
                }
            }
            catch (EventSystemException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public void closeOutputStream() throws IOException {
        out.close();
    }

    public ObjectName getObjectName() throws MalformedObjectNameException {
        return new ObjectName("org.lwes:name=SequenceFileHandler");
    }
}
