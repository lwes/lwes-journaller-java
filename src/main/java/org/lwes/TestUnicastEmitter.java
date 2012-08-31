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
import org.lwes.emitter.UnicastEventEmitter;

import java.io.IOException;
import java.net.InetAddress;

public class TestUnicastEmitter extends UnicastEventEmitter implements Runnable {

    private static transient Log log = LogFactory.getLog(TestUnicastEmitter.class);

    @Option(name="-a", aliases = "--address")
    private String uniAddress;

    @Option(name="-p", aliases="--uniPort")
    private int uniPort;

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
        setAddress(InetAddress.getByName(uniAddress));
        setPort(uniPort);
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
        TestUnicastEmitter te = new TestUnicastEmitter();
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

    public String getUniAddr() {
        return uniAddress;
    }

    public void setUniAddr(String uniAddress) {
        this.uniAddress = uniAddress;
    }

    public int getUniPort() {
        return uniPort;
    }

    public void setUniPort(int port) {
        this.uniPort = port;
    }

    public int getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }
}
