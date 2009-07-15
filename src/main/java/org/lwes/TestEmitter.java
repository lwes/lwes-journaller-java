package org.lwes;
/**
 * User: fmaritato
 * Date: Apr 20, 2009
 */

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lwes.emitter.MulticastEventEmitter;

import java.net.InetAddress;

public class TestEmitter extends MulticastEventEmitter implements Runnable {

    private static transient Log log = LogFactory.getLog(TestEmitter.class);

    private int number = 1;
    private int seconds = 1;
    private int breakSeconds = 0;
    private boolean sendRotate = false;

    private static Options options;

    static {
        options = new Options();
        options.addOption("m", "multicast-address", true, "Multicast address.");
        options.addOption("p", "port", true, "Multicast Port.");
        options.addOption("i", "interface", true, "Multicast Interface.");
        options.addOption("e", "esf-file", true, "Event definition file.");
        options.addOption("n", "number", true, "Number of events per second to emit.");
        options.addOption("s", "seconds", true, "Number of seconds to emit events.");
        options.addOption("b", "break", true, "Number of seconds to break between event bursts.");
        options.addOption("r", "rotate", false, "Send a Command::Rotate event.");
        options.addOption("t", "ttl", true, "Set the time to live value on the socket");
        options.addOption("h", "help", false, "Print this message.");
    }

    public void run() {
        try {
            initialize();

            // if we are supposed to send a rotate message, just do that and exit.
            if (isSendRotate()) {
                Event evt = createEvent("Command::Rotate", false);
                emit(evt);
            }
            else {
                for (int i = 0; i < getSeconds(); i++) {
                    for (int j = 0; j < getNumber(); j++) {
                        Event evt = createEvent("MyEvent", false);
                        evt.setString("field", "Testing-"+i+"-"+j);
                        evt.setInt32("count", j);
                        evt.setInt32("num", i);
                        emit(evt);
                    }

                    Thread.sleep(getBreakSeconds() * 1000);
                }
            }
        }
        catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

    public static void main(String[] args) {

        TestEmitter te = new TestEmitter();
        try {
            CommandLineParser parser = new PosixParser();
            CommandLine line = parser.parse(options, args);

            if (line.hasOption("h") || line.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("lwes-journaller", options);
                Runtime.getRuntime().exit(1);
            }
            if (line.hasOption("m") || line.hasOption("multicast-address")) {
                te.setMulticastAddress(InetAddress.getByName(line.getOptionValue("m") == null ?
                                                             line.getOptionValue("multicast-address") :
                                                             line.getOptionValue("m")));
            }
            if (line.hasOption("p") || line.hasOption("port")) {
                te.setMulticastPort(Integer.parseInt(line.getOptionValue("p") == null ?
                                                     line.getOptionValue("port") :
                                                     line.getOptionValue("p")));
            }
            if (line.hasOption("i") || line.hasOption("interface")) {
                te.setInterface(InetAddress.getByName(line.getOptionValue("i") == null ?
                                                      line.getOptionValue("interface") :
                                                      line.getOptionValue("i")));
            }
            if (line.hasOption("r") || line.hasOption("rotate")) {
                te.setSendRotate(true);
            }
            if (line.hasOption("e") || line.hasOption("esf-file")) {
                te.setESFFilePath(line.getOptionValue("e") == null ?
                                  line.getOptionValue("esf-file") :
                                  line.getOptionValue("e"));
            }
            if (line.hasOption("n") || line.hasOption("number")) {
                te.setNumber(Integer.parseInt((line.getOptionValue("n") == null ?
                                               line.getOptionValue("number") :
                                               line.getOptionValue("n"))));
            }
            if (line.hasOption("b") || line.hasOption("break")) {
                te.setBreakSeconds(Integer.parseInt((line.getOptionValue("b") == null ?
                                                     line.getOptionValue("break") :
                                                     line.getOptionValue("b"))));
            }
            if (line.hasOption("s") || line.hasOption("seconds")) {
                te.setSeconds(Integer.parseInt((line.getOptionValue("s") == null ?
                                                line.getOptionValue("seconds") :
                                                line.getOptionValue("s"))));
            }
            if (line.hasOption("t") || line.hasOption("ttl")) {
                te.setTimeToLive(Integer.parseInt((line.getOptionValue("t") == null ?
                                                line.getOptionValue("ttl") :
                                                line.getOptionValue("t"))));
            }
            te.run();
        }
        catch (Exception e) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
    }

    public boolean isSendRotate() {
        return sendRotate;
    }

    public void setSendRotate(boolean sendRotate) {
        this.sendRotate = sendRotate;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public int getSeconds() {
        return seconds;
    }

    public void setSeconds(int seconds) {
        this.seconds = seconds;
    }

    public int getBreakSeconds() {
        return breakSeconds;
    }

    public void setBreakSeconds(int breakSeconds) {
        this.breakSeconds = breakSeconds;
    }
}
