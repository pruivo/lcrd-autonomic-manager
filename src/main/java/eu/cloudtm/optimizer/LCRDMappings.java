package eu.cloudtm.optimizer;

import java.util.Map;

/**
 * @author Pedro Ruivo
 * @since 1.0
 */
public class LCRDMappings {

    private final Map<String, Integer> transactionClassMap;
    private final Map<String, Integer> domainObjectClassMap;
    private final Map<Integer, Float> clusterWeightMap;

    public LCRDMappings(Map<String, Integer> transactionClassMap, Map<String, Integer> domainObjectClassMap, Map<Integer, Float> clusterWeightMap) {
        this.transactionClassMap = transactionClassMap;
        this.domainObjectClassMap = domainObjectClassMap;
        this.clusterWeightMap = clusterWeightMap;
    }

    public Map<String, Integer> getTransactionClassMap() {
        return transactionClassMap;
    }

    public Map<String, Integer> getDomainObjectClassMap() {
        return domainObjectClassMap;
    }

    public Map<Integer, Float> getClusterWeightMap() {
        return clusterWeightMap;
    }

    @Override
    public String toString() {
        return "LCRDMappings{" +
                "transactionClassMap=" + transactionClassMap +
                ", domainObjectClassMap=" + domainObjectClassMap +
                ", clusterWeightMap=" + clusterWeightMap +
                '}';
    }
}
