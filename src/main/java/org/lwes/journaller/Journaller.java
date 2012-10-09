package org.lwes.journaller;
/**
 * User: fmaritato
 * Date: Apr 14, 2009
 */

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.SocketTimeoutException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

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

public class Journaller implements Runnable, JournallerMBean {

    private static transient Log log = LogFactory.getLog(Journaller.class);

    @Option(name = "-f", aliases = "--file", usage = "File to log to")
    private String fileName;

    @Option(name = "-l", aliases = "--file-pattern", usage = "Pattern to use for file name generation")
    private String filePattern = "%tY%tm%td%tH%tM-%h";

    @Option(name = "-a", aliases = "--address", usage = "Unicast or multicast address to listen on")
    private String address = "224.1.1.11";

    @Option(name = "-i", aliases = "--multicast-interface", usage = "Multicast interface")
    private String multicastInterface;

    @Option(name = "-p", aliases = "--port", usage = "Port to listen on")
    private int port = 12345;

    @Option(name = "-t", aliases = "--ttl", usage = "Time to live")
    private int ttl = 1;

    @Option(name = "-s", aliases = "--site", usage = "Site ID")
    private int siteId = 0;

    @Option(name = "-q", aliases = "--queue-size", usage = "Max number of events to queue")
    private int queueSize = 8000;

    @Option(name = "--health-interval", usage = "Interval in seconds to emit health event")
    private int healthInterval = 60;

    @Option(name = "--gzip", usage = "Produce a gzip file")
    private boolean useGzip = false;

    @Option(name = "--sequence", usage = "Produce a sequence file")
    private boolean useSequence = false;

    private AbstractFileEventHandler eventHandler = null;
    private volatile DatagramSocket socket = null;
    private boolean initialized = false;
    private volatile boolean running = true;
    private LinkedBlockingQueue<DatagramQueueElement> queue = null;
    private MBeanServer mbs = null;
    private long dropCount = 0;

    private final Object mutex = new Object();

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

        mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            ObjectName name = new ObjectName("org.lwes:name=Journaller");
            mbs.registerMBean(this, name);
            //mbs.registerMBean(eventHandler, eventHandler.getObjectName());
        }
        catch (MalformedObjectNameException e) {
            log.error(e.getMessage(), e);
        }
        catch (NotCompliantMBeanException e) {
            log.error(e.getMessage(), e);
        }
        catch (InstanceAlreadyExistsException e) {
            log.error(e.getMessage(), e);
        }
        catch (MBeanRegistrationException e) {
            log.error(e.getMessage(), e);
        }

        queue = new LinkedBlockingQueue<DatagramQueueElement>(queueSize);

        InetAddress address = InetAddress.getByName(getAddress());
        if (address.isMulticastAddress()) {
            socket = new MulticastSocket(getPort());
            ((MulticastSocket) socket).joinGroup(address);
            socket.setSoTimeout(5000);
        }
        else {
            socket = new DatagramSocket(getPort(), address);
            socket.setSoTimeout(5000);
        }
        // If we want monitoring events *emitted* then provide the handler with the socket
        // and relevent information.
        eventHandler.setSocket(socket);
        eventHandler.setAddress(address);
        eventHandler.setPort(getPort());
        eventHandler.setHealthInterval(getHealthInterval());
        eventHandler.setSiteId(getSiteId());

        int bufSize = JournallerConstants.MAX_MSG_SIZE * 50;
        String bufSizeStr = System.getProperty("ReceiveBufferSize");
        if (bufSizeStr != null && !"".equals(bufSizeStr)) {
            bufSize = Integer.parseInt(bufSizeStr);
        }
        if (log.isDebugEnabled()) {
            log.debug("receive buffer size: " + bufSize);
        }
        socket.setReceiveBufferSize(bufSize);

        if (getMulticastInterface() != null && address.isMulticastAddress()) {
            InetAddress iface = InetAddress.getByName(getMulticastInterface());
            ((MulticastSocket) socket).setInterface(iface);
        }

        // Add a shutdown hook in case of kill or ^c
        Runtime.getRuntime().addShutdownHook(new ShutdownThread(eventHandler));

        HandlerThread handlerThread = new HandlerThread();
        Thread t = new Thread(handlerThread, "handler thread");
        t.setPriority(Thread.NORM_PRIORITY);
        t.start();

        if (log.isInfoEnabled()) {
            log.info("LWES Journaller");
            log.info("Address: " + getAddress());
            log.info("Multicast Interface: " + getMulticastInterface());
            log.info("Port: " + getPort());
            log.info("Using event handler: " + getEventHandler().getClass().getName());
            log.info("Site ID: " + getSiteId());
            log.info("Health check interval: " + getHealthInterval());
        }

        initialized = true;
    }

    public void shutdown() {
        if (log.isInfoEnabled()) {
            log.info("Got shutdown signal");
        }
        eventHandler.destroy();
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
                try {
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

                    // If we don't check the capacity here and the queue is full, it will throw
                    // an IllegalStateException and the journaller will stop writing events to the
                    // file. If the queue is full, just drop events until it empties.
                    if (queue.remainingCapacity() > 0) {
                        queue.add(element);
                    }
                    else {
                        log.error("Queue is full. Dropping events!");
                        dropCount++;
                    }
                }
                catch (SocketTimeoutException e) {
                    // Don't really care. This is here in case we want to interrupt the thread and kill
                    // the process without using a unix signal.
                }
            }
        }
        catch (Exception e) {
            log.error(e.getMessage(), e);
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

    public long getEventCount() {
        return eventHandler.getEventCount();
    }

    public int getCurrentQueueSize() {
        return queue.size();
    }

    public static void main(String[] args) {
        Journaller j = new Journaller();
        CmdLineParser parser = null;
        try {
            parser = new CmdLineParser(j);
            parser.parseArgument(args);
        }
        catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            return;
        }
        j.run();
    }

    class ShutdownThread extends Thread {

        AbstractFileEventHandler eventHandler;

        ShutdownThread(AbstractFileEventHandler eh) {
            eventHandler = eh;
        }

        public void run() {
            log.info("shutdown thread run()");
            shutdown();
        }
    }

    /**
     * Convenience method for rotating the journaller file without needing
     * to know which implementation is being used (sequence, gzip, etc.)
     *
     * @return Returns the result of the eventHandler.rotate() call.
     * @throws IOException
     */
    public boolean rotate() throws IOException {
        synchronized (mutex) {
            dropCount = 0;
        }
        return eventHandler.rotate();
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

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
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

    public long getDropCount() {
        return dropCount;
    }
}
