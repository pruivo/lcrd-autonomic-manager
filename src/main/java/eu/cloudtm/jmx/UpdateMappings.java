package eu.cloudtm.jmx;

import eu.cloudtm.optimizer.LCRDMappings;
import org.apache.log4j.Logger;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import java.util.Set;

/**
 * @author Pedro Ruivo
 * @since 1.0
 */
public class UpdateMappings implements JmxManager.MBeanConnectionAction {

    private static final Logger log = Logger.getLogger(UpdateMappings.class);
    private final JmxManager jmxManager;
    private final FenixObjectNameFinder fenixObjectNameFinder;
    private final InfinispanObjectNameFinder infinispanObjectNameFinder;
    private LCRDMappings mappings;

    public UpdateMappings(JmxManager jmxManager, FenixObjectNameFinder fenixObjectNameFinder,
                          InfinispanObjectNameFinder infinispanObjectNameFinder) {
        this.jmxManager = jmxManager;
        this.fenixObjectNameFinder = fenixObjectNameFinder;
        this.infinispanObjectNameFinder = infinispanObjectNameFinder;
    }

    public synchronized final void updateMappings(LCRDMappings mappings) {
        log.debug("Updating mappings to " + mappings);
        if (mappings == null) {
            return;
        }
        this.mappings = mappings;
        jmxManager.perform(this);
    }

    @Override
    public void perform(MBeanServerConnection connection, String hostAddress, int port) {
        log.debug("Updating mappings to " + mappings + " in " + hostAddress + "(" + port + ")");
        Set<ObjectName> lardObjectNameSet = fenixObjectNameFinder.findFenixComponent(connection, "Worker");
        log.debug("LARD found: " + lardObjectNameSet);
        if (lardObjectNameSet.isEmpty()) {
            return;
        }
        Set<ObjectName> ispnObjectNameSet = infinispanObjectNameFinder.findCacheComponent(connection, "DataPlacementManager");
        log.debug("ISPN found: " + ispnObjectNameSet);
        if (ispnObjectNameSet.isEmpty()) {
            return;
        }
        //TODO
    }

    @Override
    public String toString() {
        return "UpdateMappings{" +
                "mappings=" + mappings +
                '}';
    }
}
