package eu.cloudtm;

import eu.cloudtm.jmx.*;
import eu.cloudtm.optimizer.LCRDMappings;
import eu.cloudtm.optimizer.LCRDOptimizer;
import eu.cloudtm.stats.ProcessedSample;
import eu.cloudtm.stats.StatsCollector;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author Pedro Ruivo
 * @since 1.0
 */
public class Main {

    private static final Logger log = Logger.getLogger(Main.class);
    private final JmxManager jmxManager;
    private final StatsCollector statsCollector;
    private final LCRDOptimizer optimizer;
    private final UpdateMappings updateMappings;
    private final InfinispanObjectNameFinder infinispanObjectNameFinder;
    private final FenixObjectNameFinder fenixObjectNameFinder;
    private final DapController dapController;
    private volatile int collectionTime;

    public Main() {
        jmxManager = new JmxManager();
        infinispanObjectNameFinder = new InfinispanObjectNameFinder();
        fenixObjectNameFinder = new FenixObjectNameFinder();
        optimizer = new LCRDOptimizer();
        statsCollector = new StatsCollector(jmxManager, fenixObjectNameFinder, infinispanObjectNameFinder);
        updateMappings = new UpdateMappings(jmxManager, fenixObjectNameFinder, infinispanObjectNameFinder);
        dapController = new DapController(jmxManager, fenixObjectNameFinder);
    }

    public static void main(String[] args) throws InterruptedException {
        Main main = new Main();
        main.reloadProperties();
        //main.makeRound();
        //main.randomTest();
        //main.sendDummyData();
        main.sendPreComputedData3Clusters();
    }

    private void sendDummyData() {
        Map<String, Integer> txClassMap = new HashMap<String, Integer>();
        for (int i = 0; i < 20; ++i) {
            txClassMap.put("tx-class-" + i, i % 10);
        }
        Map<Integer, Float> clusterWeightMap = new HashMap<Integer, Float>();
        for (int i = 0; i < 10; ++i) {
            clusterWeightMap.put(i, 0.1f);
        }
        Map<String, Integer> domainClassMap = new HashMap<String, Integer>();
        for (int i = 0; i < 50; ++i) {
            domainClassMap.put("domain-class-" + i, i % 10);
        }
        LCRDMappings mappings = new LCRDMappings(txClassMap, domainClassMap, clusterWeightMap);
        updateMappings.updateMappings(mappings);
    }
    
    /*
    Generated mappings is LCRDMappings{
    transactionClassMap={
        TPCW-execute-search=2, 
        TPCW-buy-confirm-servlet=0, 
        TPCW-new-products-servlet=2, 
        TPCW-buy-request-servlet=0, 
        TPCW-promotional-processing=2, 
        TPCW-best-sellers-servlet=1, 
        TPCW-say-hello=0, 
        TPCW-product-detail-servlet=2, 
        TPCW-shopping-cart-interaction=2, 
        TPCW-order-display-servlet=0, 
        TPCW-customer-registration-servlet=0}, 
    domainObjectClassMap={
        pt.ist.fenixframework.adt.bplustree.InnerNode=2, 
        pt.ist.fenixframework.adt.bplustree.BPlusTree=2, 
        pt.ist.fenixframework.adt.bplustree.LeafNode=2, 
        pt.ist.fenixframework.example.tpcw.domain.Author=2, 
        pt.ist.fenixframework.DomainRoot=2, 
        pt.ist.fenixframework.example.tpcw.domain.Book=2, 
        pt.ist.fenixframework.example.tpcw.domain.App=2, 
        pt.ist.fenixframework.example.tpcw.domain.Country=0, 
        pt.ist.fenixframework.example.tpcw.domain.Orders=1, 
        pt.ist.fenixframework.example.tpcw.domain.ShoppingCart=2, 
        pt.ist.fenixframework.example.tpcw.domain.CCXact=1, 
        pt.ist.fenixframework.example.tpcw.domain.ShoppingCartLine=2, 
        pt.ist.fenixframework.example.tpcw.domain.OrderLine=1, 
        pt.ist.fenixframework.example.tpcw.domain.Address=0, 
        pt.ist.fenixframework.adt.bplustree.AbstractNode=2, 
        pt.ist.fenixframework.example.tpcw.domain.Customer=0}, 
    clusterWeightMap={2=0.30819854, 0=0.6109371, 1=0.080864385}}
    */
    private void sendPreComputedData3Clusters() {
        Map<String, Integer> txClassMap = new HashMap<String, Integer>();
        Map<Integer, Float> clusterWeightMap = new HashMap<Integer, Float>();
        Map<String, Integer> domainClassMap = new HashMap<String, Integer>();
        
        txClassMap.put("TPCW-execute-search", 2);
        txClassMap.put("TPCW-buy-confirm-servlet", 0);
        txClassMap.put("TPCW-new-products-servlet", 2);
        txClassMap.put("TPCW-buy-request-servlet", 0);
        txClassMap.put("TPCW-promotional-processing", 2);
        txClassMap.put("TPCW-best-sellers-servlet", 1);
        txClassMap.put("TPCW-say-hello", 0);
        txClassMap.put("TPCW-product-detail-servlet", 2);
        txClassMap.put("TPCW-shopping-cart-interaction", 2);
        txClassMap.put("TPCW-order-display-servlet", 0);
        txClassMap.put("TPCW-customer-registration-servlet", 0);
        
        clusterWeightMap.put(0, 0.6109371f);
        clusterWeightMap.put(1, 0.080864385f);
        clusterWeightMap.put(2, 0.30819854f);
        
        domainClassMap.put("pt.ist.fenixframework.adt.bplustree.InnerNode", 2);
        domainClassMap.put("pt.ist.fenixframework.adt.bplustree.BPlusTree", 2);
        domainClassMap.put("pt.ist.fenixframework.adt.bplustree.LeafNode", 2);
        domainClassMap.put("pt.ist.fenixframework.example.tpcw.domain.Author", 2);
        domainClassMap.put("pt.ist.fenixframework.DomainRoot", 2);
        domainClassMap.put("pt.ist.fenixframework.example.tpcw.domain.Book", 2);
        domainClassMap.put("pt.ist.fenixframework.example.tpcw.domain.App", 2);
        domainClassMap.put("pt.ist.fenixframework.example.tpcw.domain.Country", 0);
        domainClassMap.put("pt.ist.fenixframework.example.tpcw.domain.Orders", 1);
        domainClassMap.put("pt.ist.fenixframework.example.tpcw.domain.ShoppingCart", 2);
        domainClassMap.put("pt.ist.fenixframework.example.tpcw.domain.CCXact", 1);
        domainClassMap.put("pt.ist.fenixframework.example.tpcw.domain.ShoppingCartLine", 2);
        domainClassMap.put("pt.ist.fenixframework.example.tpcw.domain.OrderLine", 1);
        domainClassMap.put("pt.ist.fenixframework.example.tpcw.domain.Address", 0);
        domainClassMap.put("pt.ist.fenixframework.adt.bplustree.AbstractNode", 2);
        domainClassMap.put("pt.ist.fenixframework.example.tpcw.domain.Customer", 0);
        
        LCRDMappings mappings = new LCRDMappings(txClassMap, domainClassMap, clusterWeightMap);
        updateMappings.updateMappings(mappings);
    }

    private void randomTest() {
        LinkedHashMap<String, LinkedHashMap<String, Integer>> map = new LinkedHashMap<String, LinkedHashMap<String, Integer>>();
        LinkedHashMap<String, Integer> map2 = new LinkedHashMap<String, Integer>();
        map2.put("test2", 1);
        map2.put("test3", 1);
        map.put("test", map2);
        optimizer.generateClusters(map);
    }

    private void makeRound() throws InterruptedException {
        log.info("Perform a new round");
        try {
            jmxManager.openConnections();

            log.debug("Enabling DAP...");
            dapController.setDapEnabled(true);

            Thread.sleep(collectionTime * 1000);

            log.debug("Disabling DAP...");
            dapController.setDapEnabled(false);

            log.debug("Collecting statistics...");
            ProcessedSample sample = statsCollector.collectStats();
            log.debug("Statistics are " + sample);

            log.debug("Optimizing...");
            LCRDMappings mappings = optimizer.doOptimize(sample);
            log.debug("Mappings are " + mappings);

            updateMappings.updateMappings(mappings);
        } finally {
            jmxManager.closeConnections();
        }
    }

    private void reloadProperties() {
        log.info("Reloading properties...");
        Properties properties = Utils.loadProperties("config.properties");
        log.info("Properties are " + properties);
        jmxManager.update(properties);
        statsCollector.update(properties);
        infinispanObjectNameFinder.update(properties);
        fenixObjectNameFinder.update(properties);
        this.collectionTime = Integer.parseInt(properties.getProperty("collectionTime"));
    }

}
