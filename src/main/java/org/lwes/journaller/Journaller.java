package org.lwes.journaller;
/**
 * User: fmaritato
 * Date: Apr 14, 2009
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.lwes.EventSystemException;
import org.lwes.journaller.handler.AbstractFileEventHandler;
import org.lwes.journaller.handler.GZIPEventHandler;
import org.lwes.journaller.handler.NIOEventHandler;
import org.lwes.journaller.handler.SequenceFileHandler;
import org.lwes.listener.DatagramQueueElement;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Journaller implements Runnable {

    private static transient Log log = LogFactory.getLog(Journaller.class);

    @Option(name = "-f", aliases = "--file")
    private String fileName;

    @Option(name = "-l", aliases = "--file-pattern")
    private String filePattern;

    @Option(name = "-m", aliases = "--multicast-address")
    private String multicastAddress = "224.1.1.11";

    @Option(name = "i", aliases = "--multicast-interface")
    private String multicastInterface;

    @Option(name = "-p", aliases = "--port")
    private int port = 12345;

    @Option(name = "-t", aliases = "--ttl")
    private int ttl = 1;

    @Option(name = "-s", aliases = "--site")
    private int siteId = 0;

    @Option(name = "--health-interval")
    private int healthInterval = 60;

    @Option(name = "--gzip")
    private boolean useGzip = false;

    @Option(name = "--sequence")
    private boolean useSequence = false;

    private AbstractFileEventHandler eventHandler = null;
    private MulticastSocket socket = null;
    private boolean initialized = false;
    private boolean running = true;
    private LinkedBlockingQueue<DatagramQueueElement> queue = new LinkedBlockingQueue(5000);

    public Journaller() {
    }

    public void initialize() throws EventSystemException, IOException {
        String arg = getFileName() == null ? getFilePattern() : getFileName();
        if (useGzip) {
            eventHandler = new GZIPEventHandler(arg);
        }
        else if (useSequence) {
            eventHandler = new SequenceFileHandler(arg);
        }
        else {
            eventHandler = new NIOEventHandler(arg);
        }
        eventHandler.setSiteId(getSiteId());
        InetAddress address = InetAddress.getByName(getMulticastAddress());

        socket = new MulticastSocket(getPort());
        socket.joinGroup(address);

        // If we want monitoring events *emitted* then provide the handler with the socket
        // and relevent information.
        eventHandler.setSocket(socket);
        eventHandler.setMulticastAddr(address);
        eventHandler.setMulticastPort(getPort());
        eventHandler.setHealthInterval(getHealthInterval());

        int bufSize = JournallerConstants.MAX_MSG_SIZE * 50;
        String bufSizeStr = System.getProperty("MulticastReceiveBufferSize");
        if (bufSizeStr != null && !"".equals(bufSizeStr)) {
            bufSize = Integer.parseInt(bufSizeStr);
        }
        if (log.isDebugEnabled()) {
            log.debug("multicast receive buffer size: " + bufSize);
        }
        socket.setReceiveBufferSize(bufSize);

        if (getMulticastInterface() != null) {
            InetAddress iface = InetAddress.getByName(getMulticastInterface());
            socket.setInterface(iface);
        }

        // Add a shutdown hook in case of kill or ^c
        Runtime.getRuntime().addShutdownHook(new ShutdownThread(eventHandler));

        HandlerThread handlerThread = new HandlerThread();
        Thread t = new Thread(handlerThread, "handler thread");
        t.setPriority(Thread.NORM_PRIORITY);
        t.start();

        if (log.isInfoEnabled()) {
            log.info("LWES Journaller");
            log.info("Multicast Address: " + getMulticastAddress());
            log.info("Multicast Interface: " + getMulticastInterface());
            log.info("Multicast Port: " + getPort());
            log.info("Using event hander: " + getEventHandler().getClass().getName());
            log.info("Site ID: " + getSiteId());
            log.info("Health check interval: " + getHealthInterval());
        }

        initialized = true;
    }

    public void shutdown() {
        running = false;
    }

    public void run() {
        try {
            if (!initialized) {
                initialize();
            }

            byte[] buffer = new byte[65535];

            while (running) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                /* we record the time *after* the receive because it blocks */
                long receiptTime = System.currentTimeMillis();

                /* copy the data into a tight buffer so we can release the loose buffer */
                final byte[] tightBuffer = new byte[packet.getLength()];
                System.arraycopy(packet.getData(), 0, tightBuffer, 0, tightBuffer.length);
                packet.setData(tightBuffer);

                /* create an element for the queue */
                DatagramQueueElement element = new DatagramQueueElement();
                element.setPacket(packet);
                element.setTimestamp(receiptTime);

                queue.add(element);
            }
        }
        catch (Exception e) {
            log.error("Error initializing: ", e);
        }
    }

    class HandlerThread implements Runnable {
        public void run() {
            while (running) {
                DatagramQueueElement item = null;
                try {
                    item = queue.poll(1, TimeUnit.MINUTES);
                }
                catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                }
                if (item != null) {
                    try {
                        eventHandler.handleEvent(item);
                    }
                    catch (IOException e) {
                        log.error(e.getMessage(), e);
                    }
                }
            }
        }
    }

    protected void parseArguments(String[] args) throws CmdLineException {
        CmdLineParser parser = new CmdLineParser(this);
        parser.parseArgument(args);
    }

    public static void main(String[] args) {
        Journaller j = new Journaller();
        try {
            j.parseArguments(args);
        }
        catch (CmdLineException e) {
            log.error(e.getMessage(), e);
        }
        j.run();
    }

    class ShutdownThread extends Thread {

        AbstractFileEventHandler eventHandler;

        ShutdownThread(AbstractFileEventHandler eh) {
            eventHandler = eh;
        }

        public void run() {
            log.debug("shutdown thread run()");
            eventHandler.destroy();
            shutdown();
        }
    }

    public boolean isUseGzip() {
        return useGzip;
    }

    public void setUseGzip(boolean useGzip) {
        this.useGzip = useGzip;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getMulticastAddress() {
        return multicastAddress;
    }

    public void setMulticastAddress(String multicastAddress) {
        this.multicastAddress = multicastAddress;
    }

    public String getMulticastInterface() {
        return multicastInterface;
    }

    public void setMulticastInterface(String iface) {
        this.multicastInterface = iface;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public AbstractFileEventHandler getEventHandler() {
        return eventHandler;
    }

    public void setEventHandler(AbstractFileEventHandler eventHandler) {
        this.eventHandler = eventHandler;
    }

    public int getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }

    public String getFilePattern() {
        return filePattern;
    }

    public void setFilePattern(String filePattern) {
        this.filePattern = filePattern;
    }

    public int getSiteId() {
        return siteId;
    }

    public void setSiteId(int siteId) {
        this.siteId = siteId;
    }

    public int getHealthInterval() {
        return healthInterval;
    }

    public void setHealthInterval(int healthInterval) {
        this.healthInterval = healthInterval;
    }

    public boolean isUseSequence() {
        return useSequence;
    }

    public void setUseSequence(boolean useSequence) {
        this.useSequence = useSequence;
    }
}
