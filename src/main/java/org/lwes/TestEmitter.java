package org.lwes;
/**
 * User: fmaritato
 * Date: Apr 20, 2009
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.lwes.emitter.MulticastEventEmitter;

import java.io.IOException;
import java.net.InetAddress;

public class TestEmitter extends MulticastEventEmitter implements Runnable {

    private static transient Log log = LogFactory.getLog(TestEmitter.class);

    @Option(name="-m", aliases = "--multicast-address")
    private String multicastAddr;

    @Option(name="-p", aliases="--port")
    private int port;

    @Option(name="-i", aliases = "--interface")
    private String multicastInterface;

    @Option(name="-t", aliases = "--ttl")
    private int ttl = 1;

    @Option(name = "-n", aliases = "--number")
    private int number = 1;

    @Option(name = "-s", aliases = "--seconds")
    private int seconds = 1;

    @Option(name = "-b", aliases = "--break")
    private int breakSeconds = 0;

    @Option(name = "-r", aliases = "--rotate")
    private boolean sendRotate = false;

    @Override
    public void initialize() throws IOException {
        setMulticastAddress(InetAddress.getByName(multicastAddr));
        setMulticastPort(port);
        setTimeToLive(ttl);
        super.initialize();
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
                        evt.setString("field", "Testing-" + i + "-" + j);
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

    protected void parseArguments(String[] args) throws CmdLineException {
        CmdLineParser parser = new CmdLineParser(this);
        parser.parseArgument(args);
    }

    public static void main(String[] args) {
        TestEmitter te = new TestEmitter();
        try {
            te.parseArguments(args);
        }
        catch (CmdLineException e) {
            log.error(e.getMessage(), e);
        }
        te.run();
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

    public String getMulticastAddr() {
        return multicastAddr;
    }

    public void setMulticastAddr(String multicastAddress) {
        this.multicastAddr = multicastAddress;
    }

    public String getMulticastInterface() {
        return multicastInterface;
    }

    public void setMulticastInterface(String multicastInterface) {
        this.multicastInterface = multicastInterface;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }
}
