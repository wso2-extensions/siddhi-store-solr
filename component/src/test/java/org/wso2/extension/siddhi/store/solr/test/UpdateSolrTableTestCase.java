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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.awaitility.Duration;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.store.solr.exceptions.SolrClientServiceException;
import org.wso2.extension.siddhi.store.solr.impl.SiddhiSolrClient;
import org.wso2.extension.siddhi.store.solr.impl.SolrClientServiceImpl;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import java.io.IOException;
import java.sql.SQLException;

/**
 * This test class contains the tests related to update queries for Solr stores
 */
public class UpdateSolrTableTestCase {
    private static final Log log = LogFactory.getLog(UpdateSolrTableTestCase.class);
    private static SolrClientServiceImpl indexerService;


    @BeforeClass
    public static void startTest() {
        log.info("== Solr Table UPDATE tests started ==");
        indexerService = SolrClientServiceImpl.INSTANCE;
    }

    private long getDocCount(String collection)
            throws SolrClientServiceException, IOException, SolrServerException {
        SiddhiSolrClient client = indexerService.getSolrServiceClientByCollection(collection);
        SolrQuery solrQuery = new SolrQuery("*:*");
        solrQuery.setRows(0);
        return client.query(collection, solrQuery).getResults().getNumFound();
    }

    @Test
    public void updateFromTableTest1() throws InterruptedException, SQLException {
        log.info("updateFromTableTest1");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            String streams = "" +
                             "define stream StockStream (symbol string, price float, volume long); " +
                             "define stream UpdateStockStream (symbol string, price float, volume long); " +
                             "@Store(type='solr', url='localhost:9983', collection='TEST13', base" +
                             ".config='gettingstarted', shards='2', replicas='2', schema='symbol string stored, price" +
                             " float stored, volume long stored', commit.async='false')" +
                             "define table StockTable (symbol string, price float, volume long); ";
            String query = "" +
                           "@info(name = 'query1') " +
                           "from StockStream " +
                           "insert into StockTable ;" +
                           "" +
                           "@info(name = 'query2') " +
                           "from UpdateStockStream " +
                           "update StockTable " +
                           "   on StockTable.symbol == symbol ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST13", Duration.FIVE_SECONDS);
            updateStockStream.send(new Object[]{"IBM", 57.6f, 100L});
            AssertJUnit.assertEquals(3, getDocCount("TEST13"));
            indexerService.deleteCollection("TEST13");
            siddhiAppRuntime.shutdown();
        } catch (SolrClientServiceException | SolrServerException | IOException e) {
            log.error("Test case 'updateFromTableTest1' ignored due to " + e.getMessage(), e);
        }
    }

    @Test
    public void updateFromTableTest2() throws InterruptedException, SQLException {
        log.info("updateFromTableTest2");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            String streams = "" +
                             "define stream StockStream (symbol string, price float, volume long); " +
                             "define stream UpdateStockStream (symbol string, price float, volume long); " +
                             "@Store(type='solr', url='localhost:9983', collection='TEST14', base" +
                             ".config='gettingstarted', shards='2', replicas='2', schema='symbol string stored, price" +
                             " float stored, volume long stored', commit.async='false')" +
                             "define table StockTable (symbol string, price float, volume long); ";
            String query = "" +
                           "@info(name = 'query1') " +
                           "from StockStream " +
                           "insert into StockTable ;" +
                           "" +
                           "@info(name = 'query2') " +
                           "from UpdateStockStream " +
                           "update StockTable " +
                           "   on StockTable.symbol == symbol;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST14", Duration.FIVE_SECONDS);
            updateStockStream.send(new Object[]{"IBM", 57.6f, 100L});
            AssertJUnit.assertEquals(3, getDocCount("TEST14"));
            indexerService.deleteCollection("TEST14");
            siddhiAppRuntime.shutdown();
        } catch (Exception e) {
            log.error("Test case 'updateFromTableTest2' ignored due to " + e.getMessage(), e);
        }
    }

    @Test
    public void updateFromTableTest3() throws InterruptedException, SolrClientServiceException {
        log.info("updateFromTableTest3");
        final int[] inEventCount = {0};
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                         "define stream StockStream (symbol string, price float, volume long); " +
                         "define stream CheckStockStream (symbol string, volume long); " +
                         "@Store(type='solr', url='localhost:9983', collection='TEST15', base" +
                         ".config='gettingstarted', shards='2', replicas='2', schema='symbol string stored, price" +
                         " float stored, volume long stored', commit.async='false')" +
                         "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                       "@info(name = 'query1') " +
                       "from StockStream " +
                       "insert into StockTable ;" +
                       "" +
                       "@info(name = 'query2') " +
                       "from CheckStockStream[(StockTable.symbol==symbol) in StockTable] " +
                       "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        checkReceivedEvents(inEventCount, siddhiAppRuntime);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 55.6f, 100L});
        SolrTestUtils.waitTillEventsPersist(indexerService, 2, "TEST15", Duration.ONE_MINUTE);
        checkStockStream.send(new Object[]{"IBM", 100L});
        checkStockStream.send(new Object[]{"WSO2", 100L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        SolrTestUtils.waitTillVariableCountMatches(inEventCount[0], 3, Duration.ONE_MINUTE);
        AssertJUnit.assertEquals(3, inEventCount[0]);
        indexerService.deleteCollection("TEST15");
        siddhiAppRuntime.shutdown();
    }

    private void checkReceivedEvents(int[] inEventCount, SiddhiAppRuntime siddhiAppRuntime) {
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event ignored : inEvents) {
                        inEventCount[0]++;
                    }
                }
            }

        });
    }

    @Test
    public void updateFromTableTest4() throws InterruptedException, SolrClientServiceException {
        log.info("updateFromTableTest4");
        final int[] inEventCount = {0};
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                         "define stream StockStream (symbol string, price float, volume long); " +
                         "define stream CheckStockStream (symbol string, volume long); " +
                         "@Store(type='solr', url='localhost:9983', collection='TEST16', base" +
                         ".config='gettingstarted', shards='2', replicas='2', schema='symbol string stored, price" +
                         " float stored, volume long stored', commit.async='false')" +
                         "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                       "@info(name = 'query1') " +
                       "from StockStream " +
                       "insert into StockTable ;" +
                       "" +
                       "@info(name = 'query2') " +
                       "from CheckStockStream[(StockTable.symbol==symbol) in StockTable] " +
                       "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        checkOutputEvents(inEventCount, siddhiAppRuntime);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 55.6f, 100L});
        SolrTestUtils.waitTillEventsPersist(indexerService, 2, "TEST16", Duration.ONE_MINUTE);
        checkStockStream.send(new Object[]{"IBM", 100L});
        checkStockStream.send(new Object[]{"WSO2", 100L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        SolrTestUtils.waitTillVariableCountMatches(inEventCount[0], 3, Duration.ONE_MINUTE);
        indexerService.deleteCollection("TEST16");
        siddhiAppRuntime.shutdown();
    }

    private void checkOutputEvents(int[] inEventCount, SiddhiAppRuntime siddhiAppRuntime) {
        checkReceivedEvents(inEventCount, siddhiAppRuntime);
    }


    @Test
    public void updateFromTableTest5() throws InterruptedException, SolrClientServiceException {
        log.info("updateFromTableTest5");
        final int[] inEventCount = {0};
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                         "define stream StockStream (symbol string, price float, volume long); " +
                         "define stream CheckStockStream (symbol string, volume long); " +
                         "@Store(type='solr', url='localhost:9983', collection='TEST17', base" +
                         ".config='gettingstarted', shards='2', replicas='2', schema='symbol string stored, price" +
                         " float stored, volume long stored', commit.async='false')" +
                         "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                       "@info(name = 'query1') " +
                       "from StockStream " +
                       "insert into StockTable ;" +
                       "" +
                       "@info(name = 'query2') " +
                       "from CheckStockStream[(StockTable.symbol==symbol) in StockTable] " +
                       "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        checkOutputEvents(inEventCount, siddhiAppRuntime);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 55.6f, 100L});
        SolrTestUtils.waitTillEventsPersist(indexerService, 2, "TEST17", Duration.FIVE_SECONDS);
        checkStockStream.send(new Object[]{"BSD", 100L});
        checkStockStream.send(new Object[]{"WSO2", 100L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        SolrTestUtils.waitTillVariableCountMatches(inEventCount[0], 2, Duration.FIVE_SECONDS);
        AssertJUnit.assertEquals(2, inEventCount[0]);
        indexerService.deleteCollection("TEST17");
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void updateFromTableTest6() throws InterruptedException, SolrClientServiceException {
        log.info("updateFromTableTest6");
        final int[] inEventCount = {0};
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                         "define stream StockStream (symbol string, price float, volume long); " +
                         "define stream CheckStockStream (symbol string, volume long); " +
                         "@Store(type='solr', url='localhost:9983', collection='TEST18', base" +
                         ".config='gettingstarted', shards='2', replicas='2', schema='symbol string stored, price" +
                         " float stored, volume long stored', commit.async='false')" +
                         "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                       "@info(name = 'query1') " +
                       "from StockStream " +
                       "insert into StockTable ;" +
                       "" +
                       "@info(name = 'query2') " +
                       "from CheckStockStream[(StockTable.symbol==symbol) in StockTable] " +
                       "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        checkOutputEvents(inEventCount, siddhiAppRuntime);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 55.6f, 100L});
        SolrTestUtils.waitTillEventsPersist(indexerService, 2, "TEST18", Duration.FIVE_SECONDS);
        checkStockStream.send(new Object[]{"IBM", 100L});
        checkStockStream.send(new Object[]{"WSO2", 100L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        SolrTestUtils.waitTillVariableCountMatches(inEventCount[0], 3, Duration.FIVE_SECONDS);
        AssertJUnit.assertEquals(3, inEventCount[0]);
        indexerService.deleteCollection("TEST18");
        siddhiAppRuntime.shutdown();
    }


    @Test
    public void updateFromTableTest7() throws InterruptedException, SolrClientServiceException {
        log.info("updateFromTableTest7");
        final int[] inEventCount = {0};
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                         "define stream StockStream (symbol string, price float, volume long); " +
                         "define stream CheckStockStream (symbol string, price float, volume long); " +
                         "define stream UpdateStockStream (comp string, prc float, volume long); " +
                         "@Store(type='solr', url='localhost:9983', collection='TEST19', base" +
                         ".config='gettingstarted', shards='2', replicas='2', schema='symbol string stored, price" +
                         " float stored, volume long stored', commit.async='false')" +
                         "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                       "@info(name = 'query1') " +
                       "from StockStream " +
                       "insert into StockTable ;" +
                       "" +
                       "@info(name = 'query2') " +
                       "from UpdateStockStream " +
                       //TODO verify the case if only some fields are selected (e.g. no volume)
                       "select comp as symbol, prc as price, volume " +
                       "update StockTable " +
                       "   on StockTable.symbol==symbol;" +
                       "" +
                       "@info(name = 'query3') " +
                       "from CheckStockStream[(symbol==StockTable.symbol and volume==StockTable.volume" +
                       " and price<StockTable.price) in StockTable] " +
                       "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount[0]++;
                        switch (inEventCount[0]) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 150.6f, 100L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 190.6f, 100L});
                                break;
                            default:
                                AssertJUnit.assertSame(2, inEventCount[0]);
                        }
                    }
                }
            }
        });

        try {
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
            InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 185.6f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, 2, "TEST19", Duration.FIVE_SECONDS);
            checkStockStream.send(new Object[]{"IBM", 150.6f, 100L});
            checkStockStream.send(new Object[]{"WSO2", 175.6f, 100L});
            updateStockStream.send(new Object[]{"IBM", 200f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, "price:200", 1, "TEST19",
                    Duration.ONE_MINUTE);
            checkStockStream.send(new Object[]{"IBM", 190.6f, 100L});
            checkStockStream.send(new Object[]{"WSO2", 155.6f, 100L});
            AssertJUnit.assertEquals(2, inEventCount[0]);
        } finally {
            indexerService.deleteCollection("TEST19");
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void updateFromTableTest8() throws InterruptedException {
        log.info("updateFromTableTest8");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            String streams = "" +
                             "define stream StockStream (symbol string, price float, volume long); " +
                             "define stream UpdateStockStream (symbol string, price float, volume long); " +
                             "@Store(type='solr', url='localhost:9983', collection='TEST20', base" +
                             ".config='gettingstarted', shards='2', replicas='2', schema='symbol string stored, price" +
                             " float stored, volume long stored', commit.async='false')" +
                             "define table StockTable (symbol string, price float, volume long); ";
            String query = "" +
                           "@info(name = 'query1') " +
                           "from StockStream " +
                           "insert into StockTable ;" +
                           "" +
                           "@info(name = 'query2') " +
                           "from UpdateStockStream " +
                           "update StockTable " +
                           "   on StockTable.volume == volume ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST20", Duration.FIVE_SECONDS);
            updateStockStream.send(new Object[]{"IBM", 57.6f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST20", Duration.FIVE_SECONDS);
            AssertJUnit.assertEquals(3, getDocCount("TEST20"));
            indexerService.deleteCollection("TEST20");
            siddhiAppRuntime.shutdown();
        } catch (Exception e) {
            log.error("Test case 'updateFromTableTest8' ignored due to " + e.getMessage(), e);
        }
    }

    @Test
    public void updateFromTableTest9() throws InterruptedException {
        log.info("updateFromTableTest9");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            String streams = "" +
                             "define stream StockStream (symbol string, price float, volume long); " +
                             "define stream UpdateStockStream (symbol string, price float, volume long); " +
                             "@Store(type='solr', url='localhost:9983', collection='TEST21', base" +
                             ".config='gettingstarted', shards='2', replicas='2', schema='symbol string stored, price" +
                             " float stored, volume long stored', commit.async='false')" +
                             "define table StockTable (symbol string, price float, volume long); ";
            String query = "" +
                           "@info(name = 'query1') " +
                           "from StockStream " +
                           "insert into StockTable ;" +
                           "" +
                           "@info(name = 'query2') " +
                           "from UpdateStockStream " +
                           "update StockTable " +
                           "   on StockTable.volume == 100 ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST21", Duration.FIVE_SECONDS);
            updateStockStream.send(new Object[]{"IBM", 57.6f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST21", Duration.FIVE_SECONDS);

            AssertJUnit.assertEquals(3, getDocCount("TEST21"));
            indexerService.deleteCollection("TEST21");
            siddhiAppRuntime.shutdown();
        } catch (Exception e) {
            log.info("Test case 'updateFromTableTest8' ignored due to " + e.getMessage(), e);
        }
    }

    @Test
    public void updateSingleRecord()
            throws InterruptedException, SolrClientServiceException, IOException, SolrServerException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                         "define stream StockStream (symbol string, price float, volume long); " +
                         "define stream UpdateStockStream (symbol string, price float, volume long); " +
                         "@store(type='solr', url='localhost:9983', collection='TEST11', base" +
                         ".config='gettingstarted', " +
                         "shards='2', replicas='2', schema='symbol string stored, price float stored, volume long" +
                         " stored', " +
                         "commit.async='false')" +
                         "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                       "@info(name = 'query1') " +
                       "from StockStream " +
                       "insert into StockTable ;" +
                       "" +
                       "@info(name = 'query2') " +
                       "from UpdateStockStream " +
                       "update StockTable " +
                       "   on StockTable.symbol == symbol ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        try {
            siddhiAppRuntime.start();
            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST11", Duration.FIVE_SECONDS);
            updateStockStream.send(new Object[]{"IBM", 57.6f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST11", Duration.FIVE_SECONDS);
            SiddhiSolrClient client = indexerService.getSolrServiceClientByCollection("TEST11");
            SolrQuery solrQuery = new SolrQuery("*:*");
            solrQuery.setRows(0);
            long noOfDocs = client.query("TEST11", solrQuery).getResults().getNumFound();
            AssertJUnit.assertEquals(3, noOfDocs);
        } finally {
            indexerService.deleteCollection("TEST11");
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void updateMultipleRecords()
            throws InterruptedException, SolrClientServiceException, IOException, SolrServerException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                         "define stream StockStream (symbol string, price float, volume long); " +
                         "define stream UpdateStockStream (symbol string, price float, volume long); " +
                         "@store(type='solr', url='localhost:9983', collection='TEST12', base" +
                         ".config='gettingstarted', " +
                         "shards='2', replicas='2', schema='symbol string stored, price float stored, volume long" +
                         " stored', " +
                         "commit.async='false')" +
                         "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                       "@info(name = 'query1') " +
                       "from StockStream " +
                       "insert into StockTable ;" +
                       "" +
                       "@info(name = 'query2') " +
                       "from UpdateStockStream " +
                       "update StockTable " +
                       "   on StockTable.symbol == symbol ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        try {
            siddhiAppRuntime.start();
            SolrTestUtils.waitTillSolrCollection(indexerService, "TEST12", Duration.TEN_SECONDS);
            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            stockStream.send(new Object[]{"IBM", 100.6f, 200L});
            stockStream.send(new Object[]{"IBM", 100.6f, 200L});
            stockStream.send(new Object[]{"IBM", 100.6f, 200L});
            stockStream.send(new Object[]{"IBM", 100.6f, 200L});
            stockStream.send(new Object[]{"IBM", 100.6f, 200L});
            stockStream.send(new Object[]{"IBM", 100.6f, 200L});
            stockStream.send(new Object[]{"IBM", 100.6f, 200L});
            stockStream.send(new Object[]{"IBM", 100.6f, 200L});
            stockStream.send(new Object[]{"IBM", 100.6f, 200L});
            stockStream.send(new Object[]{"IBM", 100.6f, 200L});
            stockStream.send(new Object[]{"IBM", 100.6f, 200L});
            stockStream.send(new Object[]{"IBM", 100.6f, 200L});
            stockStream.send(new Object[]{"IBM", 100.6f, 200L});
            stockStream.send(new Object[]{"IBM", 100.6f, 200L});
            stockStream.send(new Object[]{"IBM", 100.6f, 200L});
            SolrTestUtils.waitTillEventsPersist(indexerService, 18, "TEST12", Duration.TEN_SECONDS);
            updateStockStream.send(new Object[]{"IBM", 00.6f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, "price:\"0.6\"", 16, "TEST12",
                    Duration.TEN_SECONDS);
            SiddhiSolrClient client = indexerService.getSolrServiceClientByCollection("TEST12");
            SolrQuery solrQuery = new SolrQuery("price:\"0.6\"");
            solrQuery.setRows(0);
            long noOfDocs = client.query("TEST12", solrQuery).getResults().getNumFound();
            Assert.assertEquals(noOfDocs, 16);
        } finally {
            indexerService.deleteCollection("TEST12");
            siddhiAppRuntime.shutdown();
        }
    }
}
