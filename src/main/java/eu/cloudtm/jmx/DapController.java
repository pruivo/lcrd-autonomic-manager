package eu.cloudtm.jmx;

import org.apache.log4j.Logger;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import java.util.Set;

import static eu.cloudtm.jmx.JmxManager.EMPTY_PARAMS;
import static eu.cloudtm.jmx.JmxManager.EMPTY_SIGNATURE;

/**
 * @author Pedro Ruivo
 * @since 1.0
 */
public class DapController implements JmxManager.MBeanConnectionAction {

    private static final Logger log = Logger.getLogger(DapController.class);
    private static final String[] ENABLE_DAP_METHODS = new String[]{
            "enableDap",
            "enableWriteStatisticCollection",
            "enableReadStatisticCollection"};
    private static final String[] DISABLE_DAP_METHODS = new String[]{
            "disableDap",
            "disableWriteStatisticCollection",
            "disableReadStatisticCollection"};
    private final JmxManager jmxManager;
    private final FenixObjectNameFinder fenixObjectNameFinder;
    private volatile boolean enabled;

    public DapController(JmxManager jmxManager, FenixObjectNameFinder fenixObjectNameFinder) {
        this.jmxManager = jmxManager;
        this.fenixObjectNameFinder = fenixObjectNameFinder;
    }

    public final void setDapEnabled(boolean enabled) {
        log.info("DAP controller: enable? " + enabled);
        this.enabled = enabled;
        jmxManager.perform(this);
    }

    @Override
    public void perform(MBeanServerConnection connection, String hostAddress, int port) {
        log.debug("DAP controller on " + hostAddress + " (" + port + ")");
        Set<ObjectName> objectNameSet = fenixObjectNameFinder.findFenixComponent(connection, "DapRemoteManager");
        log.debug("DAP controller on " + hostAddress + " (" + port + "). Found: " + objectNameSet);
        if (objectNameSet.isEmpty()) {
            return;
        }
        try {
            String[] methods = enabled ? ENABLE_DAP_METHODS : DISABLE_DAP_METHODS;
            for (String method : methods) {
                connection.invoke(objectNameSet.iterator().next(), method, EMPTY_PARAMS, EMPTY_SIGNATURE);
            }
        } catch (Exception e) {
            log.error("Error in DAP controller", e);
        }
    }

    @Override
    public String toString() {
        return "DapController{" +
                "enabled=" + enabled +
                '}';
    }
}
