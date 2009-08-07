package org.lwes.journaller;
/**
 * @author fmaritato
 */

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lwes.Event;

public class CountDeJournaller extends DeJournaller {
    private static transient Log log = LogFactory.getLog(CountDeJournaller.class);

    private int counter = 0;
    private static Options options;

    static {
        options = new Options();
        options.addOption("f", "file", true, "File to read events from.");
        options.addOption("e", "esf-file", true, "Event definition file.");
        options.addOption("h", "help", false, "Print this message.");
        options.addOption(null, "gzipped", false, "File is gzipped.");
    }

    public void run() {
        super.run();
        log.info("Found: " + counter + " events.");
    }

    @Override
    public void handleEvent(Event event) {
        counter++;
        if (log.isDebugEnabled()) {
            if (counter % 100 == 0) {
                log.debug("counter: " + counter);
            }
        }
    }

    public static void main(String[] args) {
        CountDeJournaller dj = new CountDeJournaller();
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
            if (line.hasOption("gzipped")) {
                dj.setGzipped(true);
            }
            dj.run();
        }
        catch (Exception e) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
    }
}
