package eu.cloudtm.optimizer;

import eu.cloudtm.jmx.FenixObjectNameFinder;
import eu.cloudtm.jmx.InfinispanObjectNameFinder;
import eu.cloudtm.jmx.JmxManager;
import org.apache.log4j.Logger;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author Pedro Ruivo
 * @since 1.0
 */
public class MorphOptimizer implements JmxManager.MBeanConnectionAction {

    private static final Logger log = Logger.getLogger(MorphOptimizer.class);
    private static final String AVG_PUTS_PER_TX = "AvgPutsPerWrTransaction";
    private static final String CURRENT_PROTOCOL = "CurrentProtocolId";
    private final JmxManager jmxManager;
    private final InfinispanObjectNameFinder infinispanObjectNameFinder;
    private final FenixObjectNameFinder fenixObjectNameFinder;
    private int waitingTime;
    private int numberOfCollections;
    private long numberOfCommits;
    private double avgPutsPerTx;
    private String currentProtocol;
    private boolean optimizedIspn;
    private boolean optimizedLard;
    private boolean collectStatsPhase;

    public MorphOptimizer(JmxManager jmxManager, InfinispanObjectNameFinder infinispanObjectNameFinder,
                          FenixObjectNameFinder fenixObjectNameFinder) {
        this.jmxManager = jmxManager;
        this.infinispanObjectNameFinder = infinispanObjectNameFinder;
        this.fenixObjectNameFinder = fenixObjectNameFinder;
    }

    public final void update(Properties properties) {
        this.waitingTime = Integer.parseInt(properties.getProperty("collectionTime"));
        this.numberOfCollections = Integer.parseInt(properties.getProperty("nrCollections"));
    }

    @Override
    public void perform(MBeanServerConnection connection, String hostAddress, int port) {
        if (collectStatsPhase) {
            getAvgPutsPerTx(connection);
            getCurrentProtocol(connection);
            getNumberOfCommits(connection);
        } else {
            doOptimization(connection);
        }
    }

    @SuppressWarnings("ConstantConditions")
    public void optimize() {
        optimizedIspn = false;
        optimizedLard = false;
        collectStatsPhase = true;

        String finalCurrentProtocol = null;
        double avgPutsSum = 0;
        int avgPutCount = 0;

        for (int i = 0; i < numberOfCollections; ++i) {
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(waitingTime));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }

            avgPutsPerTx = -1;
            currentProtocol = null;
            numberOfCommits = -1;
            log.debug("Perform stats collection phase for " + i);
            jmxManager.perform(this);
            if (numberOfCommits <= 0) {
                log.debug("Number of commits is zero!");
                return;
            }
            if (avgPutsPerTx >= 0) {
                avgPutsSum += avgPutsPerTx;
                avgPutCount++;
                log.debug("Current average: " + avgPutsSum / avgPutCount);
            }
            if (finalCurrentProtocol == null && currentProtocol != null) {
                finalCurrentProtocol = currentProtocol;
            }
        }

        if (avgPutCount == 0) {
            log.debug("Average Put Count is zero!");
            return;
        }

        avgPutsPerTx = avgPutsSum / avgPutCount;
        currentProtocol = finalCurrentProtocol;

        collectStatsPhase = false;
        log.debug("Perform optimization phase");
        jmxManager.perform(this);
    }

    private void doOptimization(MBeanServerConnection connection) {
        if (optimizedIspn && optimizedLard) {
            return;
        }
        String newProtocol = oracle();
        log.debug("AvgPuts=" + avgPutsPerTx + ",current=" + currentProtocol + ",new=" + newProtocol);
        if (currentProtocol.equals(newProtocol)) {
            optimizedIspn = true;
            optimizedLard = true;
            return;
        }
        Set<ObjectName> ispnObjectNameSet = infinispanObjectNameFinder.findCacheComponent(connection, "ReconfigurableReplicationManager");
        log.debug("ISPN found: " + ispnObjectNameSet);
        if (ispnObjectNameSet.isEmpty()) {
            return;
        }
        final ObjectName objectName = ispnObjectNameSet.iterator().next();
        final Object[] params = new Object[]{newProtocol, false, false};
        final String[] signature = new String[]{String.class.getName(), boolean.class.getName(), boolean.class.getName()};
        try {
            connection.invoke(objectName, "switchTo", params, signature);
            optimizedIspn = true;
        } catch (Exception e) {
            //ignored
        }

        Set<ObjectName> fenixObjectNameSet = fenixObjectNameFinder.findFenixComponent(connection, "Worker");
        log.debug("Fenix found: " + fenixObjectNameSet);
        if (fenixObjectNameSet.isEmpty()) {
            return;
        }
        final ObjectName workerObjectName = fenixObjectNameSet.iterator().next();
        final Object[] params2 = new Object[]{newProtocol};
        final String[] signature2 = new String[]{String.class.getName()};
        try {
            connection.invoke(workerObjectName, "setProtocol", params2, signature2);
            optimizedLard = true;
        } catch (Exception e) {
            //ignored
        }
    }

    private String oracle() {
        if (avgPutsPerTx < 5) {
            return "2PC";
        } else if (avgPutsPerTx >= 5 && avgPutsPerTx <= 20) {
            return "TO";
        } else {
            return "PB";
        }
    }

    private void getAvgPutsPerTx(MBeanServerConnection connection) {
        if (avgPutsPerTx >= 0) {
            return;
        }
        Set<ObjectName> ispnObjectNameSet = infinispanObjectNameFinder.findCacheComponent(connection, "ExtendedStatistics");
        log.debug("ISPN found: " + ispnObjectNameSet);
        if (ispnObjectNameSet.isEmpty()) {
            return;
        }
        final ObjectName objectName = ispnObjectNameSet.iterator().next();
        try {
            avgPutsPerTx = (Double) connection.getAttribute(objectName, AVG_PUTS_PER_TX);
            log.debug("Collected Average Put per Transaction: " + avgPutsPerTx);
        } catch (Exception e) {
            //ignored
        }
    }

    private void getCurrentProtocol(MBeanServerConnection connection) {
        if (currentProtocol != null) {
            return;
        }
        Set<ObjectName> ispnObjectNameSet = infinispanObjectNameFinder.findCacheComponent(connection, "ReconfigurableReplicationManager");
        log.debug("ISPN found: " + ispnObjectNameSet);
        if (ispnObjectNameSet.isEmpty()) {
            return;
        }
        final ObjectName objectName = ispnObjectNameSet.iterator().next();
        try {
            currentProtocol = (String) connection.getAttribute(objectName, CURRENT_PROTOCOL);
            log.debug("Collected Current Protocol: " + currentProtocol);
        } catch (Exception e) {
            //ignored
        }
    }

    private void getNumberOfCommits(MBeanServerConnection connection) {
        if (numberOfCommits >= 0) {
            return;
        }
        Set<ObjectName> ispnObjectNameSet = infinispanObjectNameFinder.findCacheComponent(connection, "Transactions");
        log.debug("ISPN found: " + ispnObjectNameSet);
        if (ispnObjectNameSet.isEmpty()) {
            return;
        }
        final ObjectName objectName = ispnObjectNameSet.iterator().next();
        try {
            numberOfCommits = (Long) connection.getAttribute(objectName, "Commits");
            log.debug("Collected number of commits: " + numberOfCommits);
        } catch (Exception e) {
            //ignored
        }
    }
}
