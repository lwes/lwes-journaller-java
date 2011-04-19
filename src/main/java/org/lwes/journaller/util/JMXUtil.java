package org.lwes.journaller.util;
/**
 * Utility for calling functions exposed by the JournallerMBean.
 *
 * @author fmaritato
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.lwes.journaller.JournallerMBean;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;

public class JMXUtil {

    private static transient Log log = LogFactory.getLog(JMXUtil.class);

    @Option(name = "--rotate", usage = "Tell the journaller to rotate its log.")
    private boolean rotate = false;

    @Option(name = "--shutdown", usage = "Tell the journaller to shut down.")
    private boolean shutdown = false;

    @Option(name = "--host", required = true, usage = "Hostname to connect to.")
    private String host;

    @Option(name = "--port", required = true, usage = "Port to connect to.")
    private String port;

    /**
     * Create a connection to the hostname:port jmx and return the JournallerMBean.
     *
     * @param remoteHostAndPort hostname:port
     * @return JournallerMBean
     * @throws IOException
     * @throws MalformedObjectNameException
     */
    protected JournallerMBean getJournallerMBean(String remoteHostAndPort) throws IOException,
                                                                                  MalformedObjectNameException {

        StringBuilder urlStr = new StringBuilder()
            .append("service:jmx:rmi:///jndi/rmi://")
            .append(remoteHostAndPort)
            .append("/jmxrmi");

        if (log.isDebugEnabled()) {
            log.debug(urlStr);
        }

        JMXServiceURL url = new JMXServiceURL(urlStr.toString());
        JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
        if (log.isDebugEnabled()) {
            log.debug("Connected to remote jmx agent");
        }
        MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
        ObjectName mbeanName = new ObjectName("org.lwes:name=Journaller");
        JournallerMBean jmb = JMX.newMBeanProxy(mbsc,
                                                mbeanName,
                                                JournallerMBean.class,
                                                true);
        return jmb;
    }

    /**
     * Tell a journaller to rotate its log file.
     *
     * @param remoteHostAndPort host:port
     * @throws IOException
     * @throws MalformedObjectNameException
     */
    public void callRotate(String remoteHostAndPort) throws IOException,
                                                            MalformedObjectNameException {

        JournallerMBean jmb = getJournallerMBean(remoteHostAndPort);
        boolean success = jmb.rotate();
        if (log.isDebugEnabled()) {
            log.debug("Rotate result: " + success);
        }

    }

    /**
     * Shut down a journaller.
     *
     * @param remoteHostAndPort host:port
     * @throws MalformedObjectNameException
     * @throws IOException
     */
    public void callShutdown(String remoteHostAndPort) throws MalformedObjectNameException,
                                                              IOException {
        JournallerMBean jmb = getJournallerMBean(remoteHostAndPort);
        jmb.shutdown();
    }

    public void run() throws MalformedObjectNameException, IOException {
        if (host == null || port == null) {
            log.error("Host and port must be specified.");
            return;
        }

        if (shutdown) {
            callShutdown(host + ":" + port);
        }
        else if (rotate) {
            callRotate(host + ":" + port);
        }
        else {
            System.err.println("No command specified.");
        }
    }

    public static void main(String[] args) throws CmdLineException,
                                                  MalformedObjectNameException,
                                                  IOException {
        JMXUtil util = new JMXUtil();
        CmdLineParser parser = new CmdLineParser(util);
        try {
            parser.parseArgument(args);
            util.run();
        }
        catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
        }
    }
}
