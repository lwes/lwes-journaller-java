package org.lwes.journaller.handler;
/**
 * @author fmaritato
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lwes.Event;
import org.lwes.journaller.DeJournaller;
import org.lwes.journaller.util.EventHandlerUtil;
import org.lwes.listener.DatagramQueueElement;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class NIOEventHandler extends AbstractFileEventHandler {

    private static transient Log log = LogFactory.getLog(NIOEventHandler.class);

    private final Object semaphore = new Object();
    private FileChannel channel = null;
    private FileOutputStream out = null;

    private ByteBuffer headerBuffer = ByteBuffer.allocateDirect(DeJournaller.MAX_HEADER_SIZE);
    private ByteBuffer bodyBuffer = ByteBuffer.allocateDirect(DeJournaller.MAX_BODY_SIZE);

    public NIOEventHandler() {
    }

    public NIOEventHandler(String filePattern) throws IOException {
        setFilenamePattern(filePattern);
        generateFilename();
        createFileHandles();

        headerBuffer.clear();
        bodyBuffer.clear();
    }

    protected void rotate() throws IOException {
        channel.close();
        out.close();
        generateFilename();
        createFileHandles();
    }

    protected void createFileHandles() throws IOException {
        out = new FileOutputStream(getFilename(), true);
        if (log.isDebugEnabled()) {
            log.debug("using file: "+getFilename());
        }
        channel = out.getChannel();
    }

    public void destroy() {
        synchronized (semaphore) {
            try {
                channel.close();
                out.close();
            }
            catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public void handleEvent(DatagramQueueElement element) throws IOException {
        synchronized (semaphore) {
            DatagramPacket packet = element.getPacket();
            if (isRotateEvent(packet.getData()) &&
                !tooSoonToRotate(System.currentTimeMillis())) {
                rotate();
            }
            else {
                EventHandlerUtil.writeHeader(packet.getLength(),
                                             element.getTimestamp(),
                                             packet.getAddress(),
                                             packet.getPort(),
                                             getSiteId(),
                                             headerBuffer);

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
    }

    public void handleEvent(Event event) {
        synchronized (semaphore) {
            log.debug("Processing event: " + event.getEventName());
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
                    EventHandlerUtil.writeHeader(event, headerBuffer);
                    headerBuffer.flip();
                    channel.write(headerBuffer);
                    bodyBuffer.put(event.serialize());
                    bodyBuffer.flip();
                    channel.write(bodyBuffer);
                    out.flush();
                    headerBuffer.clear();
                    bodyBuffer.clear();
                }
                catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    public String getFileExtension() {
        return ".log";
    }

}
