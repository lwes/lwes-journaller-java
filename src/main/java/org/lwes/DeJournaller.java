package org.lwes;
/**
 * A Class that will "dejournal" files written to by the GZIPEventHandler.  If you want to
 * do something other than write the events to stdout, subclass this and override handleEvent.
 *
 * User: fmaritato
 * Date: Apr 16, 2009
 */

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lwes.db.EventTemplateDB;
import org.lwes.serializer.Deserializer;
import org.lwes.serializer.DeserializerState;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;

public class DeJournaller implements Runnable {
    private transient Log log = LogFactory.getLog(DeJournaller.class);

    public static final int MAX_HEADER_SIZE = 22;
    public static final int MAX_BODY_SIZE = 65513;
    public static final int MAX_MSG_SIZE = MAX_HEADER_SIZE + MAX_BODY_SIZE;

    private String fileName;
    private String esfFile;

    private static Options options;

    static {
        options = new Options();
        options.addOption("f", "file", true, "File to read events from.");
        options.addOption("e", "esf-file", true, "Event definition file.");
        options.addOption("h", "help", false, "Print this message.");
    }

    public DeJournaller() {
    }

    public void run() {

        EventTemplateDB evtTemplate = new EventTemplateDB();
        DeserializerState state = new DeserializerState();

        if (getEsfFile() != null) {
            evtTemplate.setESFFile(new File(getEsfFile()));
        }
        evtTemplate.initialize();

        DataInputStream in = null;
        try {
            byte[] headerData = new byte[MAX_HEADER_SIZE];
            byte[] eventData = new byte[MAX_BODY_SIZE];

            in = new DataInputStream(new GZIPInputStream(new FileInputStream(fileName)));
            while (true) {
                state.reset();

                // read header
                in.readFully(headerData, 0, MAX_HEADER_SIZE);

                int size = 0;
                long time = 0l;
                int port = 0;
                int siteId = 0;

                ByteBuffer buf = ByteBuffer.allocate(MAX_HEADER_SIZE);
                buf = buf.put(headerData);
                buf.position(0); // reset to beginning

                byte[] shortBuf = new byte[2];
                buf.get(shortBuf);
                size = Deserializer.deserializeUINT16(state, shortBuf);                          
                if (size < 0 || size > MAX_MSG_SIZE) {
                    // something is wrong
                    log.info("error reading header info. Size was "+size);
                    break;
                }

                time = buf.getLong();

                byte[] ipbytes = new byte[4];
                buf.get(ipbytes);
                InetAddress ip = InetAddress.getByAddress(ipbytes);

                state.reset();
                buf.get(shortBuf);
                port = Deserializer.deserializeUINT16(state, shortBuf);

                state.reset();
                buf.get(shortBuf);
                siteId = Deserializer.deserializeUINT16(state, shortBuf);

                byte[] unused = new byte[4];
                buf.get(unused);
                
                if (log.isDebugEnabled()) {
                    log.debug("size: " + size);
                    log.debug("time: " + time);
                    log.debug("ip: " + ip);
                    log.debug("port: " + port);
                    log.debug("siteId: " + siteId);
                }
                
                // Now read in the event                
                in.readFully(eventData, 0, size);
                Event evt = new Event(eventData, false, evtTemplate);
                handleEvent(evt);
            }
        }
        catch (EOFException e) {
            // this is normal. Just catch and ignore.
        }
        catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        finally {
            if (in != null) {
                try {
                    in.close();
                }
                catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }

    }

    /**
     * Subclass and override this method if you want to do something other than print the
     * events to the logging system. For example, you may want to write these events to a
     * database.
     *
     * @param event
     */
    public void handleEvent(Event event) {
        if (log.isDebugEnabled()) {
            log.debug("EVENT: " + event);
        }
    }

    public static void main(String[] args) {

        DeJournaller dj = new DeJournaller();
        try {
            CommandLineParser parser = new PosixParser();
            CommandLine line = parser.parse(options, args);

            if (line.hasOption("h") || line.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("lwes-journaller", options);
                Runtime.getRuntime().exit(1);
            }
            if (line.hasOption("f") || line.hasOption("file")) {
                dj.setFileName(line.getOptionValue("f") == null ?
                               line.getOptionValue("file") :
                               line.getOptionValue("f"));
            }
            if (line.hasOption("e") || line.hasOption("esf-file")) {
                dj.setEsfFile(line.getOptionValue("e") == null ?
                              line.getOptionValue("esf-file") :
                              line.getOptionValue("e"));
            }

            dj.run();
        }
        catch (Exception e) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getEsfFile() {
        return esfFile;
    }

    public void setEsfFile(String esfFile) {
        this.esfFile = esfFile;
    }
}
