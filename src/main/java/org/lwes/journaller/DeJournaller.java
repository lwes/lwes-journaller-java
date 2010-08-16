package org.lwes.journaller;
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
import org.lwes.Event;
import org.lwes.db.EventTemplateDB;
import org.lwes.journaller.util.EventHandlerUtil;
import org.lwes.serializer.DeserializerState;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

public class DeJournaller implements Runnable, JournallerConstants {
    private transient Log log = LogFactory.getLog(DeJournaller.class);

    private boolean gzipped;
    private String fileName;
    private String esfFile;
    private boolean validate = false;

    protected static Options options;

    static {
        options = new Options();
        options.addOption("f", "file", true, "File to read events from.");
        options.addOption("e", "esf-file", true, "Event definition file.");
        options.addOption("h", "help", false, "Print this message.");
        options.addOption("g", "gzipped", false, "Event log is gzipped.");
        options.addOption("v", "validate", false, "Validate events");
    }

    public DeJournaller() {
    }

    public void run() {

        if (fileName == null || "".equals(fileName)) {
            log.error("File name was not specified");
            return;
        }

        EventTemplateDB evtTemplate = new EventTemplateDB();
        DeserializerState state = new DeserializerState();

        if (getEsfFile() != null) {
            evtTemplate.setESFFile(new File(getEsfFile()));
        }
        log.debug("esf: " + evtTemplate.getESFFile());
        log.debug("validate: " + validate);
        evtTemplate.initialize();

        DataInputStream in = null;
        try {
            log.debug("Opening file: " + fileName);
            if (gzipped) {
                in = new DataInputStream(new GZIPInputStream(new FileInputStream(fileName)));
            }
            else {
                in = new DataInputStream(new FileInputStream(fileName));
            }
            Event evt;
            while ((evt = EventHandlerUtil.readEvent(in, state, evtTemplate, validate)) != null) {
                state.reset();
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
            done();
        }

    }

    /**
     * This method is called after all events in the file have been processed.
     */
    public void done() {
        if (log.isDebugEnabled()) {
            log.debug("done");
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
        System.out.println(event.toOneLineString());
    }

    public static void main(String[] args) {

        DeJournaller dj = new DeJournaller();
        try {
            CommandLineParser parser = new PosixParser();
            CommandLine line = parser.parse(options, args);

            if (line.hasOption("h") || line.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("dejournaller", options);
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
            if (line.hasOption("g") || line.hasOption("gzipped")) {
                dj.setGzipped(true);
            }
            if (line.hasOption("v") || line.hasOption("validate")) {
                dj.setValidate(true);
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

    public boolean isGzipped() {
        return gzipped;
    }

    public void setGzipped(boolean gzipped) {
        this.gzipped = gzipped;
    }

    public boolean isValidate() {
        return validate;
    }

    public void setValidate(boolean validate) {
        this.validate = validate;
    }
}
