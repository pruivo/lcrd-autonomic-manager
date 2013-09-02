package eu.cloudtm.jmx;

import eu.cloudtm.Utils;
import org.apache.log4j.Logger;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @author Pedro Ruivo
 * @since 1.0
 */
public class JmxManager {

    public static final String[] EMPTY_SIGNATURE = new String[0];
    public static final Object[] EMPTY_PARAMS = new Object[0];
    private static final JmxMachine[] EMPTY_MACHINES = new JmxMachine[0];
    private static final Logger log = Logger.getLogger(JmxManager.class);
    private static final String JMX_URL_FORMAT = "service:jmx:rmi:///jndi/rmi://%s:%s/jmxrmi";
    protected JmxMachine[] machines;

    public synchronized final void update(Properties properties) {
        String allIps = properties.getProperty("jmx.ips");
        if (allIps == null || allIps.isEmpty()) {
            machines = EMPTY_MACHINES;
            log.info("Updated Ips: " + Arrays.toString(machines));
            return;
        }
        List<JmxMachine> jmxUrlsList = new ArrayList<JmxMachine>();
        for (String ipAndPort : allIps.split(",")) {
            JmxMachine machine = create(ipAndPort);
            if (machine != null) {
                jmxUrlsList.add(machine);
            }
        }
        if (jmxUrlsList.size() > 0) {
            machines = new JmxMachine[jmxUrlsList.size()];
            jmxUrlsList.toArray(machines);
        } else {
            machines = EMPTY_MACHINES;
        }
        log.info("Updated Ips: " + Arrays.toString(machines));
    }

    public synchronized final void openConnections() {
        log.debug("Try open connections to " + Arrays.toString(machines));
        if (machines == null) {
            return;
        }

        log.debug("Closing old connections...");
        closeConnections();

        log.debug("Open new connections to " + Arrays.toString(machines));
        for (JmxMachine machine : machines) {
            machine.createConnector();
        }
    }

    public synchronized final void perform(MBeanConnectionAction action) {
        log.debug("Perform " + action + " on " + Arrays.toString(machines));
        if (machines == null) {
            return;
        }
        for (JmxMachine machine : machines) {
            MBeanServerConnection connection = machine.getConnection();
            if (connection != null) {
                action.perform(connection, machine.ip, machine.port);
            } else {
                log.debug("Unable to perform " + action + " in " + machine);
            }
        }
    }

    public synchronized final void closeConnections() {
        log.debug("Try close connections to " + Arrays.toString(machines));
        if (machines == null) {
            return;
        }
        for (JmxMachine machine : machines) {
            Utils.safeClose(machine);
        }
    }

    private JmxMachine create(String ipAndPort) {
        try {
            String[] split = ipAndPort.split(":", 2);
            if (split.length != 2) {
                return null;
            }
            return new JmxMachine(InetAddress.getByName(split[0]).getHostAddress(), Integer.parseInt(split[1]));
        } catch (Exception e) {
            return null;
        }
    }

    public static interface MBeanConnectionAction {
        void perform(MBeanServerConnection connection, String hostAddress, int port);
    }

    private class JmxMachine implements Closeable {
        private final String ip;
        private final int port;
        private final String jmxUrl;
        private volatile JMXConnector connector;

        private JmxMachine(String ip, int port) {
            this.ip = ip;
            this.port = port;
            this.jmxUrl = String.format(JMX_URL_FORMAT, ip, port);
        }

        public final MBeanServerConnection getConnection() {
            try {
                if (tryConnect()) {
                    return connector.getMBeanServerConnection();
                }
            } catch (IOException e) {
                internalClose();
            }
            return null;
        }

        public final void createConnector() {
            if (connector != null) {
                return;
            }
            try {
                connector = JMXConnectorFactory.connect(new JMXServiceURL(jmxUrl));
            } catch (Exception e) {
                internalClose();
            }
        }

        @Override
        public void close() throws IOException {
            internalClose();
        }

        @Override
        public String toString() {
            return "JmxMachine{" +
                    "ip='" + ip + '\'' +
                    ", port=" + port +
                    '}';
        }

        private boolean tryConnect() throws IOException {
            createConnector();
            if (connector == null) {
                return false;
            }
            connector.connect();
            return true;
        }

        private void internalClose() {
            JMXConnector connector1 = connector;
            connector = null;
            Utils.safeClose(connector1);
        }
    }

}
