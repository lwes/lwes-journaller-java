package org.lwes.journaller.util;

/**
 * User: fmaritato
 * Date: Sep 22, 2010
 * Time: 8:39:03 PM
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lwes.journaller.JournallerMBean;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;

public class Rotate {

    private static transient Log log = LogFactory.getLog(Rotate.class);

    /**
     * @param remoteHostAndPort hostName:portNum
     * @throws IOException
     * @throws javax.management.MalformedObjectNameException
     *
     */
    public static void sendRotate(String remoteHostAndPort) throws IOException,
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
        boolean success = jmb.rotate();
        if (log.isDebugEnabled()) {
            log.debug("Rotate result: " + success);
        }
        jmxc.close();
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: Rotate <hostname:port>");
        }
        try {
            Rotate.sendRotate(args[0]);
        }
        catch (IOException e) {
            log.error(e);
        }
        catch (MalformedObjectNameException e) {
            log.error(e);
        }
    }
}
