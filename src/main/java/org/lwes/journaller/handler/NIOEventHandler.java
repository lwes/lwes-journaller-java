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

import java.io.File;
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

    public NIOEventHandler(String outFile) throws IOException {
        setFilename(outFile);
        createFileHandles();
        // TODO: not sure this is necessary
        headerBuffer.clear();
        bodyBuffer.clear();
    }

    protected void rotate() throws IOException {
        channel.close();
        out.close();
        createFileHandles();
    }

    protected void createFileHandles() throws IOException {
        setGeneratedFilename(generateFilename());
        out = new FileOutputStream(getGeneratedFilename());
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
            if (isRotateEvent(packet.getData())) {
                rotate();
            }
            else {
                EventHandlerUtil.writeHeader(packet.getLength(),
                                             element.getTimestamp(),
                                             packet.getAddress(),
                                             packet.getPort(),
                                             0, // TODO
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

    protected String generateFilename() {

        String fn = getDateString();
        File f = new File(fn + ".log");
        for (int i = 1; f.exists(); i++) {
            f = new File(fn + "-" + i + ".log");
        }

        return f.getPath();
    }

}
