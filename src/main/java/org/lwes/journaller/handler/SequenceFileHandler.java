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
import org.apache.hadoop.io.SequenceFile;
import org.lwes.journaller.JournallerConstants;
import org.lwes.journaller.util.EventHandlerUtil;
import org.lwes.listener.DatagramQueueElement;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.IOException;
import java.net.DatagramPacket;
import java.nio.ByteBuffer;

public class SequenceFileHandler extends AbstractFileEventHandler implements JournallerConstants {

    private static transient Log log = LogFactory.getLog(SequenceFileHandler.class);

    private SequenceFile.Writer out = null;
    private BytesWritable key = new BytesWritable();
    private BytesWritable value = new BytesWritable();

    public SequenceFileHandler(String filePattern) throws IOException {
        setFilenamePattern(filePattern);
        generateFilename();
        createOutputStream();
    }

    public void createOutputStream() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.getLocal(conf).getRaw();
        Path path = new Path(getFilename());
        out = SequenceFile.createWriter(fs, conf, path,
                                        BytesWritable.class,
                                        BytesWritable.class,
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
            ByteBuffer b = ByteBuffer.allocate(JournallerConstants.MAX_HEADER_SIZE);
            EventHandlerUtil.writeHeader(packet.getLength(),
                                         element.getTimestamp(),
                                         packet.getAddress(),
                                         packet.getPort(),
                                         getSiteId(),
                                         b);

            key.set(b.array(), 0, JournallerConstants.MAX_HEADER_SIZE);
            byte[] bytes = packet.getData();
            value.set(bytes, 0, bytes.length);

            synchronized (lock) {
                if (out != null) {
                    incrNumEvents();
                    out.append(key, value);
                }
            }
        }
    }

    public void closeOutputStream() throws IOException {
        if (out != null) {
            out.close();
            out = null;
        }
    }

    public ObjectName getObjectName() throws MalformedObjectNameException {
        return new ObjectName("org.lwes:name=SequenceFileHandler");
    }
}
