package org.lwes.journaller;
/**
 * User: fmaritato
 * Date: Apr 14, 2009
 */

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lwes.EventSystemException;
import org.lwes.journaller.handler.AbstractFileEventHandler;
import org.lwes.journaller.handler.GZIPEventHandler;
import org.lwes.journaller.handler.NIOEventHandler;
import org.lwes.journaller.util.FilenameFormatter;
import org.lwes.listener.DatagramQueueElement;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Journaller implements Runnable {

    private static transient Log log = LogFactory.getLog(Journaller.class);

    private String fileName;
    private String multicastAddress = "224.1.1.11";
    private String multicastInterface;
    private int port = 12345;
    private int ttl = 1;
    private AbstractFileEventHandler eventHandler = null;
    private MulticastSocket socket = null;
    private boolean useGzip = false;
    private boolean initialized = false;
    private boolean running = true;
    private HandlerThread handlerThread = null;
    private LinkedBlockingQueue<DatagramQueueElement> queue = new LinkedBlockingQueue();

    private static Options options;

    static {
        options = new Options();
        options.addOption("f", "file", true, "File to write events to.");
        options.addOption("l", "file-pattern", true, "Pattern to use for file name.");
        options.addOption("m", "multicast-address", true, "Multicast address.");
        options.addOption("p", "port", true, "Multicast Port.");
        options.addOption("i", "interface", true, "Multicast Interface.");
        options.addOption("t", "ttl", true, "Set the Time-To-Live on the socket.");
        options.addOption("h", "help", false, "Print this message.");
        options.addOption(null, "gzip", false, "Use the gzip event handler. NIO is used by default.");
    }

    public Journaller() {
    }

    public void initialize() throws EventSystemException, IOException {
        if (useGzip) {
            eventHandler = new GZIPEventHandler(getFileName());
        }
        else {
            eventHandler = new NIOEventHandler(getFileName());
        }
        InetAddress address = InetAddress.getByName(getMulticastAddress());

        socket = new MulticastSocket(getPort());
        socket.joinGroup(address);

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

        handlerThread = new HandlerThread();
        Thread t = new Thread(handlerThread, "handler thread");
        t.setPriority(Thread.NORM_PRIORITY);
        t.start();

        if (log.isInfoEnabled()) {
            log.info("LWES Journaller");
            log.info("Multicast Address: " + getMulticastAddress());
            log.info("Multicast Interface: " + getMulticastInterface());
            log.info("Multicast Port: " + getPort());
            log.info("Using event hander: " + getEventHandler().getClass().getName());
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

    public static void main(String[] args) {
        Journaller j = new Journaller();

        try {
            CommandLineParser parser = new PosixParser();
            CommandLine line = parser.parse(options, args);

            if (line.hasOption("h") || line.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("lwes-journaller", options);
                Runtime.getRuntime().exit(1);
            }
            if (line.hasOption("m") || line.hasOption("multicast-address")) {
                j.setMulticastAddress(line.getOptionValue("m") == null ?
                                      line.getOptionValue("multicast-address") :
                                      line.getOptionValue("m"));
            }
            if (line.hasOption("p") || line.hasOption("port")) {
                j.setPort(Integer.parseInt(line.getOptionValue("p") == null ?
                                           line.getOptionValue("port") :
                                           line.getOptionValue("p")));
            }
            if (line.hasOption("i") || line.hasOption("interface")) {
                j.setMulticastInterface(line.getOptionValue("i") == null ?
                                        line.getOptionValue("interface") :
                                        line.getOptionValue("i"));
            }
            if (line.hasOption("t") || line.hasOption("ttl")) {
                j.setTtl(Integer.parseInt(line.getOptionValue("t") == null ?
                                          line.getOptionValue("ttl") :
                                          line.getOptionValue("t")));
            }
            if (line.hasOption("gzip")) {
                j.setUseGzip(true);
            }

            // Use one or the other for determining file name
            if (line.hasOption("l") || line.hasOption("file-pattern")) {
                String pat = line.getOptionValue("l") == null ?
                             line.getOptionValue("file-pattern") :
                             line.getOptionValue("l");
                FilenameFormatter f = new FilenameFormatter();
                String fn = f.format(pat);
                j.setFileName(fn);
            }
            else if (line.hasOption("f") || line.hasOption("file")) {
                j.setFileName(line.getOptionValue("f") == null ?
                              line.getOptionValue("file") :
                              line.getOptionValue("f"));
            }

            j.run();
        }
        catch (NumberFormatException e) {
            log.error(e);
        }
        catch (ParseException e) {
            log.error(e);
        }
    }

    class ShutdownThread extends Thread {

        AbstractFileEventHandler eventHandler;

        ShutdownThread(AbstractFileEventHandler eh) {
            eventHandler = eh;
        }

        public void run() {
            log.debug("shutdown thread run()");
            eventHandler.destroy();
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
}
