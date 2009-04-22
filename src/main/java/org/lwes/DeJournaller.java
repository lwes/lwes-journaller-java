package org.lwes;
/**
 * A Class that will "dejournal" files written to by the GZIPEventHandler.  If you want to
 * do something other than write the events to stdout, subclass this and override handleEvent.
 *
 * User: fmaritato
 * Date: Apr 16, 2009
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.lwes.serializer.DeserializerState;
import org.lwes.db.EventTemplateDB;

import java.util.zip.GZIPInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.File;
import java.nio.ByteBuffer;
import java.net.InetAddress;

public class DeJournaller implements Runnable {
    private transient Log log = LogFactory.getLog(DeJournaller.class);

    public static final int MAX_HEADER_SIZE = 24;
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

        GZIPInputStream gzin = null;
        try {
            byte[] headerData = new byte[MAX_HEADER_SIZE];
            byte[] eventData = new byte[MAX_BODY_SIZE];

            gzin = new GZIPInputStream(new FileInputStream(fileName));
            while (true) {
                state.reset();
                // read header
                int val = gzin.read(headerData, 0, MAX_HEADER_SIZE);
                log.debug("read: "+val+" bytes for header");
                // check for EOF
                if (val == -1) {
                    break;
                }
                // This is a hack. While testing this program GZIPInputStream seemed
                // to not be smart enough to roll through the internal buffer because
                // instead of always reading MAX_HEADER_SIZE bytes it would sometimes
                // read less. I added this in to make sure the full header gets read in.
                if (val < MAX_HEADER_SIZE) {
                    val = gzin.read(headerData, val, MAX_HEADER_SIZE-val);
                    log.debug("read: "+val+" *additional* bytes");
                }
                int size = 0;
                long time = 0l;
                int port = 0;
                int siteId = 0;

                ByteBuffer buf = ByteBuffer.allocate(MAX_HEADER_SIZE);
                buf = buf.put(headerData);
                buf.position(0); // reset to beginning
                size = buf.getInt();
                if (size < 0 || size > MAX_MSG_SIZE) {
                    // something is wrong
                    log.info("error reading header info. Size was "+size);
                    break;
                }
                time = buf.getLong();
                byte[] ipbytes = new byte[4];
                buf.get(ipbytes);
                InetAddress ip = InetAddress.getByAddress(ipbytes);
                port = buf.getInt();
                siteId = buf.getInt();
                if (log.isDebugEnabled()) {
                    log.debug("size: " + size);
                    log.debug("time: " + time);
                    log.debug("ip: " + ip);
                    log.debug("port: " + port);
                    log.debug("siteId: " + siteId);
                }
                // Now read in the event
                val = gzin.read(eventData, 0, size);
                if (val == -1) {
                    log.info("Couldn't read event");
                    break;
                }
                // Same hack as above.
                if (val < size) {
                    val = gzin.read(eventData, val, size-val);
                    log.info("read an *additional* "+val+" bytes");
                }
                Event evt = new Event(eventData, false, evtTemplate);
                handleEvent(evt);
            }
        }
        catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        finally {
            if (gzin != null) {
                try {
                    gzin.close();
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
