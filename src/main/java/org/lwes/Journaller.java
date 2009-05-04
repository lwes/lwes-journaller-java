package org.lwes;
/**
 * User: fmaritato
 * Date: Apr 14, 2009
 */

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lwes.listener.DatagramEventListener;
import org.lwes.listener.EventHandler;

import java.io.IOException;
import java.net.InetAddress;

public class Journaller implements Runnable {
    private transient Log log = LogFactory.getLog(Journaller.class);

    private String fileName;
    private String multicastAddress = "224.1.1.11";
    private String multicastInterface;
    private int port = 12345;
    private EventHandler eventHandler = null;

    private static Options options;

    static {
        options = new Options();
        options.addOption("f", "file", true, "File to write events to.");
        options.addOption("m", "multicast-address", true, "Multicast address.");
        options.addOption("p", "port", true, "Multicast Port.");
        options.addOption("i", "interface", true, "Multicast Interface.");
        options.addOption("h", "help", false, "Print this message.");
        options.addOption(OptionBuilder.withLongOpt("event-handler")
                .withDescription("Fully qualified class name for event handler")
                .hasArg()
                .create());
    }

    public Journaller() {
    }

    public void initialize() throws EventSystemException, IOException {
        if (eventHandler == null) {
            eventHandler = new GZIPEventHandler(getFileName());
        }
        InetAddress address = InetAddress.getByName(getMulticastAddress());
        InetAddress iface = null;
        if (getMulticastInterface() != null) {
            iface = InetAddress.getByName(getMulticastInterface());
        }
        DatagramEventListener listener = new DatagramEventListener();
        listener.setAddress(address);
        if (iface != null) {
            listener.setInterface(iface);
        }
        listener.setPort(getPort());
        listener.addHandler(eventHandler);
        listener.initialize();
    }

    public void run() {

        try {
            initialize();

            // Add a shutdown hook in case of kill or ^c
            Runtime.getRuntime().addShutdownHook(new ShutdownThread(eventHandler));

            if (log.isDebugEnabled()) {
                log.debug("LWES Journaller");
                log.debug("Multicast Address: " + getMulticastAddress());
                log.debug("Multicast Interface: " + getMulticastInterface());
                log.debug("Multicast Port: " + getPort());
                log.debug("Using event hander: " + getEventHandler().getClass().getName());
            }

            // keep this thread busy
            while (true) {
                try {
                    Thread.sleep(1000);
                }
                catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
        catch (Exception e) {
            log.error("Error initializing: ", e);
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
            if (line.hasOption("f") || line.hasOption("file")) {
                j.setFileName(line.getOptionValue("f") == null ?
                              line.getOptionValue("file") :
                              line.getOptionValue("f"));
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
            if (line.hasOption("event-handler")) {
                String ehName = line.getOptionValue("event-handler");
                Class classdef = Class.forName(ehName);
                j.setEventHandler((EventHandler) classdef.newInstance());
            }

            j.run();
        }
        catch (Exception e) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
    }

    class ShutdownThread extends Thread {

        EventHandler eventHandler;

        ShutdownThread(EventHandler eh) {
            eventHandler = eh;
        }

        public void run() {
            log.debug("shutdown thread run()");
            eventHandler.destroy();
        }
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

    public EventHandler getEventHandler() {
        return eventHandler;
    }

    public void setEventHandler(EventHandler eventHandler) {
        this.eventHandler = eventHandler;
    }
}
