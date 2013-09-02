package eu.cloudtm.optimizer;

import eu.cloudtm.stats.ProcessedSample;
import org.apache.log4j.Logger;
import pt.ist.clustering.LDA.LDA;
import pt.ist.clustering.LDA.LDA_ExtendedResult;

import java.util.LinkedHashMap;

/**
 * @author Pedro Ruivo
 * @since 1.0
 */
public class LCRDOptimizer {

    private static final Logger log = Logger.getLogger(LCRDOptimizer.class);
    private LinkedHashMap<String, Integer> txClusterMap;// txID - clusterID map
    private LinkedHashMap<Integer, Integer> txIDClusterMap;// LDA_tx_ID - clusterID map
    private LinkedHashMap<String, Integer> txIDMap;// txID - LDA_tx_ID
    private LinkedHashMap<String, Integer> domainIDMap;// domClass - LDA_DomClass_ID
    private LinkedHashMap<Integer, String> reverseTxIDMap;// LDA_tx_ID - txID
    private LinkedHashMap<Integer, String> reverseDomainIDMap;//  LDA_DomClass_ID - domClass
    private LinkedHashMap<Integer, Float> clusterWeight;//normalized load (sum of all loads = 1) expected to be generated in every cluster
    private LinkedHashMap<String, Integer> primaryDataClusters;// domClass - primary cluster ID
    private LinkedHashMap<String, Integer> secondaryDataClusters;// domClass - secondary cluster ID

    public LCRDMappings doOptimize(ProcessedSample processedSample) {
        log.debug("Optimize based on " + processedSample);
        LinkedHashMap<String, Double> txInvokeFrequency = processedSample.getTxInvokeFrequency();
        LinkedHashMap<String, Double> txResponseTime = processedSample.getTxResponseTime();
        LinkedHashMap<String, Float> txWeight = calculateTxWeight(txInvokeFrequency, txResponseTime);
        int clusterID;

        log.debug("Generating clusters...");
        generateClusters(processedSample.getDataAccessFrequencies());

        if (txIDMap == null || txIDMap.isEmpty()) {
            log.debug("No clusters has been generated!");
            return null;
        }

        log.debug("Generating clusters weight...");
        clusterWeight = new LinkedHashMap<Integer, Float>();

        for (String s : txWeight.keySet()) {
            clusterID = txClusterMap.get(s);
            if (clusterWeight.containsKey(clusterID))
                clusterWeight.put(clusterID, clusterWeight.get(clusterID) + txWeight.get(s));
            else clusterWeight.put(clusterID, txWeight.get(s));
        }

        LCRDMappings mappings = new LCRDMappings(txClusterMap, primaryDataClusters, clusterWeight);
        log.debug("Generated mappings is " + mappings);
        return mappings;
    }

    //<TransactionID,<DomainClassID,AccessFrequency>>
    public void generateClusters(LinkedHashMap<String, LinkedHashMap<String, Integer>> dataAccessFrequencies) {
        if (dataAccessFrequencies.isEmpty()) {
            return;
        }
        int txLDA = 0;
        int domainLDA = 1;
        int numberOfClusters = 0;

        LinkedHashMap<Integer, LinkedHashMap<Integer, Integer>> ldaInput = new LinkedHashMap<Integer, LinkedHashMap<Integer, Integer>>();
        LinkedHashMap<Integer, Integer> temp;

        txIDMap = new LinkedHashMap<String, Integer>();
        domainIDMap = new LinkedHashMap<String, Integer>();
        reverseTxIDMap = new LinkedHashMap<Integer, String>();
        reverseDomainIDMap = new LinkedHashMap<Integer, String>();

        for (String txID : dataAccessFrequencies.keySet()) {
            txIDMap.put(txID, txLDA);
            reverseTxIDMap.put(txLDA, txID);

            temp = new LinkedHashMap<Integer, Integer>();

            for (String domainClass : dataAccessFrequencies.get(txID).keySet()) {
                if (!domainIDMap.containsKey(domainClass)) {
                    domainIDMap.put(domainClass, domainLDA);
                    reverseDomainIDMap.put(domainLDA, domainClass);

                    domainLDA++;
                }
                temp.put(domainIDMap.get(domainClass), dataAccessFrequencies.get(txID).get(domainClass));
            }
            ldaInput.put(txLDA, temp);

            txLDA++;
        }

        txClusterMap = new LinkedHashMap<String, Integer>();
        LDA_ExtendedResult ldaResult = LDA.generateOptimalLDAResult(ldaInput);
        //txIDClusterMap = LDA.generateOptimalLDA(ldaInput);
        txIDClusterMap = ldaResult.getTransactionClusters();
        int[][] domainDataPlacement = ldaResult.getTop2DataPlacementClusters();//int[numberOfDomainTypes][2]
        primaryDataClusters = new LinkedHashMap<String, Integer>();
        secondaryDataClusters = new LinkedHashMap<String, Integer>();

        for (int i = 0; i < domainDataPlacement.length; i++) {
            primaryDataClusters.put(reverseDomainIDMap.get(i + 1), domainDataPlacement[i][0]);
            secondaryDataClusters.put(reverseDomainIDMap.get(i + 1), domainDataPlacement[i][1]);
        }


        int t = 0;
        for (String txID : dataAccessFrequencies.keySet()) {
            log.debug("looking up " + txID + ", that goes into cluster " + txIDClusterMap.get(t));
            txClusterMap.put(txID, txIDClusterMap.get(t));
            if (numberOfClusters <= (txIDClusterMap.get(t) + 1)) numberOfClusters = txIDClusterMap.get(t) + 1;
            t++;
        }
    }

    private LinkedHashMap<String, Float> calculateTxWeight(LinkedHashMap<String, Double> txInvokeFrequency,
                                                           LinkedHashMap<String, Double> txResponseTime) {
        LinkedHashMap<String, Float> normalizedWeight = new LinkedHashMap<String, Float>();
        LinkedHashMap<String, Float> temp = new LinkedHashMap<String, Float>();
        float totalWeight = 0;
        float txWeight;

        for (String txID : txInvokeFrequency.keySet()) {
            txWeight = (float) (txInvokeFrequency.get(txID) * txResponseTime.get(txID));
            totalWeight += txWeight;
            temp.put(txID, txWeight);
        }

        for (String txID : txInvokeFrequency.keySet()) {
            normalizedWeight.put(txID, temp.get(txID) / totalWeight);
        }

        return normalizedWeight;
    }

}
