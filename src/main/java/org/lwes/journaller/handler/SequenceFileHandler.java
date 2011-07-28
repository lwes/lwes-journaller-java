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
    private SequenceFile.Writer tmp = null;
    private SequenceFile.Writer old = null;
    private BytesWritable key = new BytesWritable();
    private BytesWritable value = new BytesWritable();

    public SequenceFileHandler(String filePattern) throws IOException {
        setFilenamePattern(filePattern);
        String fn = generateFilename();
        createOutputStream(fn);
        swapOutputStream();
        setFilename(fn);
    }

    public void createOutputStream(String filename) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.getLocal(conf).getRaw();
        Path path = new Path(filename);
        tmp = SequenceFile.createWriter(fs, conf, path,
                                        BytesWritable.class,
                                        BytesWritable.class,
                                        SequenceFile.CompressionType.BLOCK);
    }

    public void swapOutputStream() {
        old = out;
        out = tmp;
        tmp = null;
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
        if (old != null) {
            old.close();
            old = null;
        }
    }

    public ObjectName getObjectName() throws MalformedObjectNameException {
        return new ObjectName("org.lwes:name=SequenceFileHandler");
    }
}
