/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extension.siddhi.store.solr.test;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.stream.input.InputHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.awaitility.Duration;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.store.solr.impl.SolrClientServiceImpl;

/**
 * This class represents the tests related to delete queries in Solr Store implementation
 */
public class DeleteFromSolrTableTestCase {
    private static final Log log = LogFactory.getLog(DeleteFromSolrTableTestCase.class);
    private static SolrClientServiceImpl indexerService;

    @BeforeClass
    public static void startTest() {
        log.info("== Solr Table DELETE tests started ==");
        indexerService = SolrClientServiceImpl.INSTANCE;
    }

    @Test
    public void deleteFromSolrTableTest1() throws InterruptedException {
        log.info("deleteFromSolrTableTest1");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            String streams = "" +
                             "define stream StockStream (symbol string, price float, volume long); " +
                             "define stream DeleteStockStream (symbol string, price float, volume long); " +
                             "@Store(type='solr', url='localhost:9983', collection='TEST34', base" +
                             ".config='gettingstarted', shards='2', replicas='2', schema='symbol string stored, price" +
                             " float stored, volume long stored', commit.async='false')" +
                             "define table StockTable (symbol string, price float, volume long); ";
            String query = "" +
                           "@info(name = 'query1') " +
                           "from StockStream " +
                           "insert into StockTable ;" +
                           "" +
                           "@info(name = 'query2') " +
                           "from DeleteStockStream " +
                           "delete StockTable " +
                           "   on StockTable.symbol == symbol ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"IBM", 75.6F, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST34", Duration.FIVE_SECONDS);
            deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
            deleteStockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            SolrTestUtils.waitTillVariableCountMatches(SolrTestUtils.getDocCount(indexerService,
                                                                                 "TEST34"), 0, Duration.FIVE_SECONDS);
            Assert.assertEquals(SolrTestUtils.getDocCount(indexerService, "TEST34"), 0 ,
                                "Deletion failed");

            indexerService.deleteCollection("TEST34");
            siddhiAppRuntime.shutdown();
        } catch (Exception e) {
            log.info("Test case 'deleteFromSolrTableTest1' ignored due to " + e.getMessage(), e);
        }
    }


    @Test
    public void deleteFromSolrTableTest2() throws InterruptedException {
        log.info("deleteFromSolrTableTest2");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            String streams = "" +
                             "define stream StockStream (symbol string, price float, volume long); " +
                             "define stream DeleteStockStream (symbol string, price float, volume long); " +
                             "@Store(type='solr', url='localhost:9983', collection='TEST35', base" +
                             ".config='gettingstarted', shards='2', replicas='2', schema='symbol string stored, price" +
                             " float stored, volume long stored', commit.async='false')" +
                             "define table StockTable (symbol string, price float, volume long); ";
            String query = "" +
                           "@info(name = 'query1') " +
                           "from StockStream " +
                           "insert into StockTable ;" +
                           "" +
                           "@info(name = 'query2') " +
                           "from DeleteStockStream " +
                           "delete StockTable " +
                           "   on symbol == StockTable.symbol ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"IBM", 75.6F, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST35", Duration.FIVE_SECONDS);
            deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
            deleteStockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            SolrTestUtils.waitTillVariableCountMatches(SolrTestUtils.getDocCount(indexerService,
                                                                                 "TEST35"), 0, Duration.FIVE_SECONDS);
            Assert.assertEquals(SolrTestUtils.getDocCount(indexerService, "TEST35"),
                                0, "Deletion failed");
            indexerService.deleteCollection("TEST35");
            siddhiAppRuntime.shutdown();
        } catch (Exception e) {
            log.info("Test case 'deleteFromSolrTableTest2' ignored due to " + e.getMessage(), e);
        }
    }


    @Test
    public void deleteFromSolrTableTest3() throws InterruptedException {
        log.info("deleteFromSolrTableTest3");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            String streams = "" +
                             "define stream StockStream (symbol string, price float, volume long); " +
                             "define stream DeleteStockStream (symbol string, price float, volume long); " +
                             "@Store(type='solr', url='localhost:9983', collection='TEST36', base" +
                             ".config='gettingstarted', shards='2', replicas='2', schema='symbol string stored, price" +
                             " float stored, volume long stored', commit.async='false')" +
                             "define table StockTable (symbol string, price float, volume long); ";
            String query = "" +
                           "@info(name = 'query1') " +
                           "from StockStream " +
                           "insert into StockTable ;" +
                           "" +
                           "@info(name = 'query2') " +
                           "from DeleteStockStream " +
                           "delete StockTable " +
                           "   on StockTable.symbol == 'IBM'  ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"IBM", 75.6F, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, 2, "TEST36", Duration.FIVE_SECONDS);
            stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
            SolrTestUtils.waitTillVariableCountMatches(SolrTestUtils.getDocCount(indexerService,
                                                                                 "TEST36"), 2, Duration.FIVE_SECONDS);
            Assert.assertEquals(SolrTestUtils.getDocCount(indexerService, "TEST36"),
                                2, "Deletion failed");
            indexerService.deleteCollection("TEST36");
            siddhiAppRuntime.shutdown();
        } catch (Exception e) {
            log.info("Test case 'deleteFromSolrTableTest3' ignored due to " + e.getMessage(), e);
        }
    }

    @Test
    public void deleteFromSolrTableTest4() throws InterruptedException {
        log.info("deleteFromSolrTableTest4");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            String streams = "" +
                             "define stream StockStream (symbol string, price float, volume long); " +
                             "define stream DeleteStockStream (symbol string, price float, volume long); " +
                             "@Store(type='solr', url='localhost:9983', collection='TEST37', base" +
                             ".config='gettingstarted', shards='2', replicas='2', schema='symbol string stored, price" +
                             " float stored, volume long stored', commit.async='false')" +
                             "define table StockTable (symbol string, price float, volume long); ";
            String query = "" +
                           "@info(name = 'query1') " +
                           "from StockStream " +
                           "insert into StockTable ;" +
                           "" +
                           "@info(name = 'query2') " +
                           "from DeleteStockStream " +
                           "delete StockTable " +
                           "   on 'IBM' == StockTable.symbol  ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"IBM", 75.6F, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST37", Duration.FIVE_SECONDS);
            deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
            SolrTestUtils.waitTillVariableCountMatches(SolrTestUtils.getDocCount(indexerService,
                                                                                 "TEST37"), 2, Duration.FIVE_SECONDS);
            Assert.assertEquals(SolrTestUtils.getDocCount(indexerService, "TEST37"),
                                2, "Deletion failed");
            indexerService.deleteCollection("TEST37");
            siddhiAppRuntime.shutdown();
        } catch (Exception e) {
            log.info("Test case 'deleteFromSolrTableTest4' ignored due to " + e.getMessage(), e);
        }
    }

    @Test
    public void deleteFromSolrTableTest5() throws InterruptedException {
        log.info("deleteFromSolrTableTest5");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            String streams = "" +
                             "define stream StockStream (symbol string, price float, volume long); " +
                             "define stream DeleteStockStream (symbol string, price float, volume long); " +
                             "@Store(type='solr', url='localhost:9983', collection='TEST38', base" +
                             ".config='gettingstarted', shards='2', replicas='2', schema='symbol string stored, price" +
                             " float stored, volume long stored', commit.async='false')" +
                             "define table StockTable (symbol string, price float, volume long); ";
            String query = "" +
                           "@info(name = 'query1') " +
                           "from StockStream " +
                           "insert into StockTable ;" +
                           "" +
                           "@info(name = 'query2') " +
                           "from DeleteStockStream " +
                           "delete StockTable " +
                           "   on 'IBM' == StockTable.symbol  ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"IBM", 75.6F, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST38", Duration.FIVE_SECONDS);
            deleteStockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            SolrTestUtils.waitTillVariableCountMatches(SolrTestUtils.getDocCount(indexerService,
                                                                                 "TEST38"), 2, Duration.FIVE_SECONDS);
            Assert.assertEquals(SolrTestUtils.getDocCount(indexerService, "TEST38"),
                                2, "Deletion failed");
            indexerService.deleteCollection("TEST38");
            siddhiAppRuntime.shutdown();
        } catch (Exception e) {
            log.info("Test case 'deleteFromSolrTableTest5' ignored due to " + e.getMessage(), e);
        }

    }

    @Test
    public void deleteFromSolrTableTest6() throws InterruptedException {
        log.info("deleteFromSolrTableTest6");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            String streams = "" +
                             "define stream StockStream (symbol string, price float, volume long); " +
                             "define stream DeleteStockStream (symbol string, price float, volume long); " +
                             "@Store(type='solr', url='localhost:9983', collection='TEST39', base" +
                             ".config='gettingstarted', shards='2', replicas='2', schema='symbol string stored, price" +
                             " float stored, volume long stored', commit.async='false')" +
                             "define table StockTable (symbol string, price float, volume long); ";
            String query = "" +
                           "@info(name = 'query1') " +
                           "from StockStream " +
                           "insert into StockTable ;" +
                           "" +
                           "@info(name = 'query2') " +
                           "from DeleteStockStream " +
                           "delete StockTable " +
                           "   on StockTable.symbol == 'IBM'  ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"IBM", 75.6F, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST39", Duration.FIVE_SECONDS);
            deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
            SolrTestUtils.waitTillVariableCountMatches(SolrTestUtils.getDocCount(indexerService, "TEST39"),
                                                       2, Duration.FIVE_SECONDS);
            Assert.assertEquals(SolrTestUtils.getDocCount(indexerService, "TEST39"),
                                2, "Deletion failed");
            indexerService.deleteCollection("TEST39");
            siddhiAppRuntime.shutdown();
        } catch (Exception e) {
            log.info("Test case 'deleteFromSolrTableTest6' ignored due to " + e.getMessage(), e);
        }
    }


    @Test
    public void deleteFromSolrTableTest7() throws InterruptedException {
        log.info("deleteFromSolrTableTest7");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            String streams = "" +
                             "define stream StockStream (symbol string, price float, volume long); " +
                             "define stream DeleteStockStream (symbol string, price float, volume long); " +
                             "@Store(type='solr', url='localhost:9983', collection='TEST40', base" +
                             ".config='gettingstarted', shards='2', replicas='2', schema='symbol string stored, price" +
                             " float stored, volume long stored', commit.async='false')" +
                             "define table StockTable (symbol string, price float, volume long); ";
            String query = "" +
                           "@info(name = 'query1') " +
                           "from StockStream " +
                           "insert into StockTable ;" +
                           "" +
                           "@info(name = 'query2') " +
                           "from DeleteStockStream " +
                           "delete StockTable " +
                           "on StockTable.symbol==symbol and StockTable.price > price and  " +
                           "StockTable.volume == volume  ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"IBM", 75.6F, 100L});
            stockStream.send(new Object[]{"IBM", 57.6F, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST40", Duration.FIVE_SECONDS);
            deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
            SolrTestUtils.waitTillVariableCountMatches(SolrTestUtils.getDocCount(indexerService, "TEST40"),
                                                       2, Duration.FIVE_SECONDS);
            Assert.assertEquals(SolrTestUtils.getDocCount(indexerService, "TEST40"),
                                2, "Deletion failed");
            indexerService.deleteCollection("TEST40");
            siddhiAppRuntime.shutdown();
        } catch (Exception e) {
            log.info("Test case 'deleteFromSolrTableTest7' ignored due to " + e.getMessage(), e);
        }
    }

    @Test
    public void deleteFromSolrTableTest8() throws InterruptedException {
        log.info("deleteFromSolrTableTest8");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            String streams = "" +
                             "define stream StockStream (symbol string, price float, volume long); " +
                             "define stream DeleteStockStream (symbol string, price float, volume long); " +
                             "@Store(type='solr', url='localhost:9983', collection='TEST41', base" +
                             ".config='gettingstarted', shards='2', replicas='2', schema='symbol string stored, price" +
                             " float stored, volume long stored', commit.async='false')" +
                             "define table StockTable (symbol string, price float, volume long); ";
            String query = "" +
                           "@info(name = 'query1') " +
                           "from StockStream " +
                           "insert into StockTable ;" +
                           "" +
                           "@info(name = 'query2') " +
                           "from DeleteStockStream " +
                           "delete StockTable " +
                           "   on StockTable.symbol=='IBM' and StockTable.price > 50 and  " +
                           "StockTable.volume == volume  ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"IBM", 75.6F, 100L});
            stockStream.send(new Object[]{"IBM", 57.6F, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST41", Duration.FIVE_SECONDS);
            deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
            SolrTestUtils.waitTillVariableCountMatches(SolrTestUtils.getDocCount(indexerService, "TEST41"),
                                                       1, Duration.FIVE_SECONDS);
            Assert.assertEquals(SolrTestUtils.getDocCount(indexerService, "TEST41"),
                                1, "Deletion failed");
            indexerService.deleteCollection("TEST41");
            siddhiAppRuntime.shutdown();
        } catch (Exception e) {
            log.info("Test case 'deleteFromSolrTableTest8' ignored due to " + e.getMessage(), e);
        }
    }


    @Test
    public void deleteFromSolrTableTest10() throws InterruptedException {
        log.info("deleteFromSolrTableTest10");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            String streams = "" +
                             "define stream StockStream (symbol string, price float, volume long); " +
                             "define stream DeleteStockStream (symbol string, price float, volume long); " +
                             "@Store(type='solr', url='localhost:9983', collection='TEST42', base" +
                             ".config='gettingstarted', shards='2', replicas='2', schema='symbol string stored, price" +
                             " float stored, volume long stored', commit.async='false')" +
                             "define table StockTable (symbol string, price float, volume long); ";
            String query = "" +
                           "@info(name = 'query1') " +
                           "from StockStream " +
                           "insert into StockTable ;" +
                           "" +
                           "@info(name = 'query2') " +
                           "from DeleteStockStream " +
                           "delete StockTable " +
                           "   on StockTable.symbol == symbol ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"IBM", 75.6F, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST42", Duration.FIVE_SECONDS);
            deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
            SolrTestUtils.waitTillVariableCountMatches(SolrTestUtils.getDocCount(indexerService, "TEST42"),
                                                       2, Duration.FIVE_SECONDS);
            Assert.assertEquals(SolrTestUtils.getDocCount(indexerService, "TEST42"),
                                2, "Deletion failed");
            indexerService.deleteCollection("TEST42");
            siddhiAppRuntime.shutdown();
        } catch (Exception e) {
            log.info("Test case 'deleteFromSolrTableTest10' ignored due to " + e.getMessage(), e);
        }
    }

    @Test
    public void deleteFromSolrTableTest11() throws InterruptedException {
        log.info("deleteFromSolrTableTest11");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            String streams = "" +
                             "define stream StockStream (symbol string, price float, volume long); " +
                             "define stream DeleteStockStream (symbol string, price float, volume long); " +
                             "@Store(type='solr', url='localhost:9983', collection='TEST43', base" +
                             ".config='gettingstarted', shards='2', replicas='2', schema='symbol string stored, price" +
                             " float stored, volume long stored', commit.async='false')" +
                             "define table StockTable (symbol string, price float, volume long); ";
            String query = "" +
                           "@info(name = 'query1') " +
                           "from StockStream " +
                           "insert into StockTable ;" +
                           "" +
                           "@info(name = 'query2') " +
                           "from DeleteStockStream " +
                           "delete StockTable " +
                           "   on StockTable.symbol == symbol ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"IBM", 75.6F, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST43", Duration.FIVE_SECONDS);
            deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
            SolrTestUtils.waitTillVariableCountMatches(SolrTestUtils.getDocCount(indexerService, "TEST43"),
                                                       2, Duration.FIVE_SECONDS);
            Assert.assertEquals(SolrTestUtils.getDocCount(indexerService, "TEST43"),
                                2, "Deletion failed");
            stockStream.send(new Object[]{null, 45.5F, 100L});
            indexerService.deleteCollection("TEST43");
            siddhiAppRuntime.shutdown();
            try {
                siddhiManager.createSiddhiAppRuntime(streams + query);
            } catch (NullPointerException ex) {
                Assert.fail("Cannot Process null values in bloom filter");
            }
        } catch (Exception e) {
            log.info("Test case 'deleteFromSolrTableTest11' ignored due to " + e.getMessage(), e);
        }
    }
}
