package eu.cloudtm.stats;

import eu.cloudtm.jmx.FenixObjectNameFinder;
import eu.cloudtm.jmx.InfinispanObjectNameFinder;
import eu.cloudtm.jmx.JmxManager;
import org.apache.log4j.Logger;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import java.util.*;

/**
 * @author Pedro Ruivo
 * @since 1.0
 */
public class StatsCollector implements JmxManager.MBeanConnectionAction {

    private static final Logger log = Logger.getLogger(StatsCollector.class);
    private static final String[] EMPTY_TX_CLASSES = new String[0];
    private static final String ARRIVAL_RATE = "getAvgTxArrivalRateForTxClass";
    private static final String RESPONSE_TIME = "getAvgResponseTimeForTxClass";
    private static final String DAP_READ_ACCESS_DATA = "DapReadAccessData";
    private static final String DAP_WRITE_ACCESS_DATA = "DapWriteAccessData";
    private static final String[] SIGNATURE = new String[]{String.class.getName()};
    private final JmxManager jmxManager;
    private final FenixObjectNameFinder fenixObjectNameFinder;
    private final InfinispanObjectNameFinder infinispanObjectNameFinder;
    private final List<Stats> statsList;
    private String[] transactionClasses;

    public StatsCollector(JmxManager jmxManager, FenixObjectNameFinder fenixObjectNameFinder,
                          InfinispanObjectNameFinder infinispanObjectNameFinder) {
        this.jmxManager = jmxManager;
        this.fenixObjectNameFinder = fenixObjectNameFinder;
        this.infinispanObjectNameFinder = infinispanObjectNameFinder;
        statsList = new ArrayList<Stats>(16);
    }

    public synchronized final void update(Properties properties) {
        String txClassList = properties.getProperty("infinispan.transactionClasses");
        if (txClassList == null || txClassList.isEmpty()) {
            transactionClasses = EMPTY_TX_CLASSES;
            log.info("Transaction classes are " + Arrays.toString(transactionClasses));
            return;
        }
        transactionClasses = txClassList.split(",");
        log.info("Transaction classes are " + Arrays.toString(transactionClasses));
    }

    @Override
    public void perform(MBeanServerConnection connection, String hostAddress, int port) {
        log.debug("Trying to collect stats from " + hostAddress + "(" + port + ")");
        Set<ObjectName> lardObjectNameSet = fenixObjectNameFinder.findFenixComponent(connection, "DapRemoteManager");
        log.debug("DAP found: " + lardObjectNameSet);
        if (lardObjectNameSet.isEmpty()) {
            return;
        }
        Set<ObjectName> ispnObjectNameSet = infinispanObjectNameFinder.findCacheComponent(connection, "ExtendedStatistics");
        log.debug("ISPN found: " + ispnObjectNameSet);
        if (ispnObjectNameSet.isEmpty()) {
            return;
        }
        ObjectName DAPRemoteManager = lardObjectNameSet.iterator().next();
        ObjectName extendedStatistics = ispnObjectNameSet.iterator().next();

        log.debug("DAP=" + DAPRemoteManager + ", ISPN=" + extendedStatistics);

        Stats stats = new Stats();
        try {
            stats.readData = (String) connection.getAttribute(DAPRemoteManager, DAP_READ_ACCESS_DATA);
            stats.writeData = (String) connection.getAttribute(DAPRemoteManager, DAP_WRITE_ACCESS_DATA);
            stats.responseTime = new long[transactionClasses.length];
            stats.arrivalRate = new double[transactionClasses.length];
            for (int i = 0; i < transactionClasses.length; ++i) {
                stats.arrivalRate[i] = (Double) connection.invoke(extendedStatistics, ARRIVAL_RATE,
                        new Object[]{transactionClasses[i]}, SIGNATURE);
                stats.responseTime[i] = (Long) connection.invoke(extendedStatistics, RESPONSE_TIME,
                        new Object[]{transactionClasses[i]}, SIGNATURE);
            }
            statsList.add(stats);
            log.debug("Added " + stats);
        } catch (Exception e) {
            log.error("Exception while collection stats from " + hostAddress + "(" + port + ")", e);
        }
    }

    public final ProcessedSample collectStats() {
        log.debug("Collecting stats...");
        clear();
        jmxManager.perform(this);
        return new ProcessedSample(getTxInvokeFrequency(), getTxResponseTime(), getDataAccessFrequencies());
    }

    private void clear() {
        statsList.clear();
    }

    private LinkedHashMap<String, LinkedHashMap<String, Integer>> getDataAccessFrequencies() {
        LinkedHashMap<String, LinkedHashMap<String, Integer>> result =
                new LinkedHashMap<String, LinkedHashMap<String, Integer>>();
        LinkedHashMap<String, Integer> contextStats;
        String[] contexts;
        String contextName;
        String[] splitContext;
        String[] tokens;
        String domainAttribute;
        String domainClass;
        String frequency;
        List<String> dataList = new ArrayList<String>(statsList.size() * 2);
        for (Stats stats : statsList) {
            dataList.add(stats.readData);
            dataList.add(stats.writeData);
        }

        log.debug("Parsing " + dataList);

        for (String toParse : dataList) {
            //toParse = readData;
            contexts = toParse.split("#");

            log.debug("Contexts: " + Arrays.toString(contexts));

            for (String context : contexts) {
                splitContext = context.split(":");
                if (splitContext.length == 1) {
                    log.debug("Ignoring '" + context + "' since it has no data");
                    continue;
                }
                contextName = splitContext[0].split("_")[0];
                tokens = splitContext[1].split(";");

                if (result.containsKey(contextName)) {
                    contextStats = result.get(contextName);
                } else {
                    contextStats = new LinkedHashMap<String, Integer>();// domainClass - accessFrequency
                    result.put(contextName, contextStats);
                }

                for (String token : tokens) {
                    //fullyQualifiedDomainClassName.attributeName=accessFrequency
                    domainAttribute = token.split("=")[0];
                    frequency = token.split("=")[1];
                    domainClass = domainAttribute.substring(0, domainAttribute.lastIndexOf("."));

                    if (contextStats.containsKey(domainClass))
                        contextStats.put(domainClass, contextStats.get(domainClass) + (new Integer(frequency)));
                    else
                        contextStats.put(domainClass, new Integer(frequency));
                }
            }
        }
        return result;
    }

    private LinkedHashMap<String, Double> getTxInvokeFrequency() {
        LinkedHashMap<String, Double> result = new LinkedHashMap<String, Double>();

        for (int i = 0; i < transactionClasses.length; ++i) {
            long sum = 0;
            int count = 0;
            for (Stats stats : statsList) {
                if (stats.arrivalRate[i] != 0) {
                    sum += stats.arrivalRate[i];
                    count++;
                }
            }
            result.put(transactionClasses[i], count == 0 ? 0 : sum * 1.0 / count);
        }

        return result;
    }

    private LinkedHashMap<String, Double> getTxResponseTime() {
        LinkedHashMap<String, Double> result = new LinkedHashMap<String, Double>();

        for (int i = 0; i < transactionClasses.length; ++i) {
            long sum = 0;
            int count = 0;
            for (Stats stats : statsList) {
                if (stats.responseTime[i] != 0) {
                    sum += stats.responseTime[i];
                    count++;
                }
            }
            result.put(transactionClasses[i], count == 0 ? 0 : sum * 1.0 / count);
        }

        return result;
    }

    private class Stats {
        private String writeData;
        private String readData;
        private double[] arrivalRate;
        private long[] responseTime;

        @Override
        public String toString() {
            return "Stats{" +
                    "writeData='" + writeData + '\'' +
                    ", readData='" + readData + '\'' +
                    ", arrivalRate=" + Arrays.toString(arrivalRate) +
                    ", responseTime=" + Arrays.toString(responseTime) +
                    '}';
        }
    }

    @Override
    public String toString() {
        return "StatsCollector{" +
                "transactionClasses=" + Arrays.toString(transactionClasses) +
                '}';
    }
}
