package eu.cloudtm.stats;

import java.util.LinkedHashMap;

/**
 * @author Pedro Ruivo
 * @since 1.0
 */
public class ProcessedSample {

    private final LinkedHashMap<String, Double> txInvokeFrequency;
    private final LinkedHashMap<String, Double> txResponseTime;
    private final LinkedHashMap<String, LinkedHashMap<String, Integer>> dataAccesses;

    public ProcessedSample(LinkedHashMap<String, Double> txInvokeFrequency, LinkedHashMap<String, Double> txResponseTime,
                           LinkedHashMap<String, LinkedHashMap<String, Integer>> dataAccesses) {
        this.txInvokeFrequency = txInvokeFrequency;
        this.txResponseTime = txResponseTime;
        this.dataAccesses = dataAccesses;
    }

    public LinkedHashMap<String, Double> getTxInvokeFrequency() {
        return txInvokeFrequency;
    }

    public LinkedHashMap<String, Double> getTxResponseTime() {
        return txResponseTime;
    }

    public LinkedHashMap<String, LinkedHashMap<String, Integer>> getDataAccessFrequencies() {
        return dataAccesses;
    }

    @Override
    public String toString() {
        return "ProcessedSample{" +
                "txInvokeFrequency=" + txInvokeFrequency +
                ", txResponseTime=" + txResponseTime +
                ", dataAccesses=" + dataAccesses +
                '}';
    }
}
