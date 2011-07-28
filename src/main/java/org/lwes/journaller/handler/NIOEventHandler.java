package org.lwes.journaller.handler;
/**
 * @author fmaritato
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lwes.journaller.DeJournaller;
import org.lwes.journaller.util.EventHandlerUtil;
import org.lwes.listener.DatagramQueueElement;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class NIOEventHandler extends AbstractFileEventHandler {

    private static transient Log log = LogFactory.getLog(NIOEventHandler.class);

    private FileChannel channel = null;
    private FileChannel tmpChannel = null;
    private FileChannel oldChannel = null;

    private FileOutputStream out = null;
    private FileOutputStream tmp = null;
    private FileOutputStream old = null;

    private ByteBuffer headerBuffer = ByteBuffer.allocateDirect(DeJournaller.MAX_HEADER_SIZE);
    private ByteBuffer bodyBuffer = ByteBuffer.allocateDirect(DeJournaller.MAX_BODY_SIZE);

    public NIOEventHandler() {
    }

    public NIOEventHandler(String filePattern) throws IOException {
        setFilenamePattern(filePattern);
        String fn = generateFilename();
        createOutputStream(fn);
        swapOutputStream();
        setFilename(fn);

        headerBuffer.clear();
        bodyBuffer.clear();
    }

    public void createOutputStream(String filename) throws IOException {
        tmp = new FileOutputStream(filename, true);
        if (log.isDebugEnabled()) {
            log.debug("using file: " + getFilename());
        }
        tmpChannel = tmp.getChannel();
    }

    public void handleEvent(DatagramQueueElement element) throws IOException {
        DatagramPacket packet = element.getPacket();

        EventHandlerUtil.writeHeader(packet.getLength(),
                                     element.getTimestamp(),
                                     packet.getAddress(),
                                     packet.getPort(),
                                     getSiteId(),
                                     headerBuffer);

        synchronized (lock) {
            headerBuffer.flip();
            channel.write(headerBuffer);
            bodyBuffer.put(packet.getData());
            bodyBuffer.flip();
            channel.write(bodyBuffer);
            out.flush();
            headerBuffer.clear();
            bodyBuffer.clear();
        }
    }

    public String getFileExtension() {
        return ".log";
    }

    public void swapOutputStream() {
        old = out;
        out = tmp;
        tmp = null;
        oldChannel = channel;
        channel = tmpChannel;
        tmpChannel = null;
    }

    public void closeOutputStream() throws IOException {
        if (oldChannel != null) {
            oldChannel.close();
            oldChannel = null;
        }
        if (old != null) {
            old.flush();
            old.close();
            old = null;
        }
    }

    public ObjectName getObjectName() throws MalformedObjectNameException {
        return new ObjectName("org.lwes:name=NIOEventHandler");
    }
}
