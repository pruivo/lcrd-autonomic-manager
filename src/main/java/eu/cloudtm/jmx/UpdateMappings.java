package eu.cloudtm.jmx;

import eu.cloudtm.optimizer.LCRDMappings;
import org.apache.log4j.Logger;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import java.util.Map;
import java.util.Set;

/**
 * @author Pedro Ruivo
 * @since 1.0
 */
public class UpdateMappings implements JmxManager.MBeanConnectionAction {

    private static final String[] SIGNATURE = new String[]{
            Map.class.getName(),
            Map.class.getName()
    };
    private static final Logger log = Logger.getLogger(UpdateMappings.class);
    private final JmxManager jmxManager;
    private final FenixObjectNameFinder fenixObjectNameFinder;
    private final InfinispanObjectNameFinder infinispanObjectNameFinder;
    private LCRDMappings mappings;
    private boolean ispnMapsUpdated = false;
    private boolean lardMapsUpdated = false;

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
        this.ispnMapsUpdated = false;
        this.lardMapsUpdated = false;
        this.mappings = mappings;
        jmxManager.perform(this);
    }

    @Override
    public void perform(MBeanServerConnection connection, String hostAddress, int port) {
        if (!ispnMapsUpdated) {
            log.debug("Updating ISPN mappings to " + mappings + " in " + hostAddress + "(" + port + ")");
            Set<ObjectName> ispnObjectNameSet = infinispanObjectNameFinder.findCacheComponent(connection, "DataPlacementManager");
            log.debug("ISPN found: " + ispnObjectNameSet);
            if (ispnObjectNameSet.isEmpty()) {
                return;
            }
            final ObjectName dataPlacementObjectName = ispnObjectNameSet.iterator().next();
            final Object[] params = new Object[]{mappings.getTransactionClassMap(), mappings.getClusterWeightMap()};
            ispnMapsUpdated = update(connection, dataPlacementObjectName, "setLCRDMappings", params);
        }

        if (!lardMapsUpdated) {
            log.debug("Updating LARD mappings to " + mappings + " in " + hostAddress + "(" + port + ")");
            Set<ObjectName> lcrdObjectNameSet = fenixObjectNameFinder.findFenixComponent(connection, "LCRDLoadBalancePolicy");
            log.debug("LCRD found: " + lcrdObjectNameSet);
            if (lcrdObjectNameSet.isEmpty()) {
                return;
            }
            final ObjectName LCRDObjectName = lcrdObjectNameSet.iterator().next();
            final Object[] params = new Object[]{mappings.getDomainObjectClassMap(), mappings.getClusterWeightMap()};
            lardMapsUpdated = update(connection, LCRDObjectName, "updateMappings", params);
        }
    }

    @Override
    public String toString() {
        return "UpdateMappings{" +
                "mappings=" + mappings +
                '}';
    }

    private boolean update(MBeanServerConnection connection, ObjectName objectName, String method, Object[] params) {
        try {
            connection.invoke(objectName, method, params, SIGNATURE);
            return true;
        } catch (Exception e) {
            log.error("Error in Update Mappings", e);
        }
        return false;
    }
}
