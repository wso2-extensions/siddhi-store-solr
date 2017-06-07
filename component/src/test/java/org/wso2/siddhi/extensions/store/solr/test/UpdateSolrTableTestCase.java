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

package org.wso2.siddhi.extensions.store.solr.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.extensions.store.solr.exceptions.SolrClientServiceException;
import org.wso2.siddhi.extensions.store.solr.impl.SiddhiSolrClient;
import org.wso2.siddhi.extensions.store.solr.impl.SolrClientServiceImpl;

import java.io.IOException;
import java.sql.SQLException;

/**
 * This test class contains the tests related to update queries for Solr stores
 */
public class UpdateSolrTableTestCase {
    private static final Log log = LogFactory.getLog(UpdateSolrTableTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;
    private static SolrClientServiceImpl indexerService;


    @Before
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
    }

    @BeforeClass
    public static void startTest() {
        log.info("== Solr Table UPDATE tests started ==");
        indexerService = SolrClientServiceImpl.getInstance();
    }

    @AfterClass
    public static void shutdown() throws SolrClientServiceException {

        if (indexerService == null) {
            throw new SolrClientServiceException("Indexer Service cannot be loaded!");
        }
        try {
            indexerService.deleteCollection("TEST11");
            indexerService.deleteCollection("TEST12");
            indexerService.deleteCollection("TEST13");
            indexerService.deleteCollection("TEST14");
            indexerService.deleteCollection("TEST15");
            indexerService.deleteCollection("TEST16");
            indexerService.deleteCollection("TEST17");
            indexerService.deleteCollection("TEST18");
            indexerService.deleteCollection("TEST19");
            indexerService.deleteCollection("TEST20");
            indexerService.deleteCollection("TEST21");
        } finally {
            indexerService.destroy();
        }
        log.info("== Solr Table UPDATE tests completed ==");
    }

    private long getDocCount(String query, String collection)
            throws SolrClientServiceException, IOException, SolrServerException {
        SiddhiSolrClient client = indexerService.getSolrServiceClient();
        SolrQuery solrQuery = new SolrQuery(query);
        solrQuery.setRows(0);
        long noOfDocs = client.query(collection, solrQuery).getResults().getNumFound();
        return noOfDocs;
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

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = executionPlanRuntime.getInputHandler("UpdateStockStream");
            executionPlanRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            updateStockStream.send(new Object[]{"IBM", 57.6f, 100L});
            Thread.sleep(1000);

            Assert.assertEquals("Update failed", 3, getDocCount("*:*", "TEST13"));
            executionPlanRuntime.shutdown();

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

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = executionPlanRuntime.getInputHandler("UpdateStockStream");
            executionPlanRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            updateStockStream.send(new Object[]{"IBM", 57.6f, 100L});
            Thread.sleep(1000);

            Assert.assertEquals("Update failed", 3, getDocCount("*:*", "TEST14"));
        } catch (Exception e) {
            log.error("Test case 'updateFromTableTest2' ignored due to " + e.getMessage(), e);
        }
    }

    @Test
    public void updateFromTableTest3() throws InterruptedException {
        log.info("updateFromTableTest3");
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

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        executionPlanRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                    }
                    eventArrived = true;
                }

            }

        });

        InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = executionPlanRuntime.getInputHandler("CheckStockStream");
        executionPlanRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 55.6f, 100L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        checkStockStream.send(new Object[]{"WSO2", 100L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        Thread.sleep(1000);
        Assert.assertEquals("Number of success events", 3, inEventCount);
        Assert.assertEquals("Event arrived", true, eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void updateFromTableTest4() throws InterruptedException {
        log.info("updateFromTableTest4");
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

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        executionPlanRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                    }
                    eventArrived = true;
                }

            }

        });

        InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = executionPlanRuntime.getInputHandler("CheckStockStream");
        executionPlanRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 55.6f, 100L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        checkStockStream.send(new Object[]{"WSO2", 100L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        Thread.sleep(1000);

        Assert.assertEquals("Number of success events", 3, inEventCount);
        Assert.assertEquals("Event arrived", true, eventArrived);
        executionPlanRuntime.shutdown();
    }


    @Test
    public void updateFromTableTest5() throws InterruptedException {
        log.info("updateFromTableTest5");
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

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        executionPlanRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                    }
                    eventArrived = true;
                }

            }

        });

        InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = executionPlanRuntime.getInputHandler("CheckStockStream");
        executionPlanRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 55.6f, 100L});
        checkStockStream.send(new Object[]{"BSD", 100L});
        checkStockStream.send(new Object[]{"WSO2", 100L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        Thread.sleep(1000);

        Assert.assertEquals("Number of success events", 2, inEventCount);
        Assert.assertEquals("Event arrived", true, eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void updateFromTableTest6() throws InterruptedException {
        log.info("updateFromTableTest6");
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

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        executionPlanRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                    }
                    eventArrived = true;
                }

            }

        });

        InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = executionPlanRuntime.getInputHandler("CheckStockStream");
        executionPlanRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 55.6f, 100L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        checkStockStream.send(new Object[]{"WSO2", 100L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        Thread.sleep(1000);

        Assert.assertEquals("Number of success events", 3, inEventCount);
        Assert.assertEquals("Event arrived", true, eventArrived);
        executionPlanRuntime.shutdown();
    }


    @Test
    public void updateFromTableTest7() throws InterruptedException {
        log.info("updateFromTableTest7");
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

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        executionPlanRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertArrayEquals(new Object[]{"IBM", 150.6f, 100L}, event.getData());
                                break;
                            case 2:
                                Assert.assertArrayEquals(new Object[]{"IBM", 190.6f, 100L}, event.getData());
                                break;
                            default:
                                Assert.assertSame(2, inEventCount);
                        }
                    }
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = executionPlanRuntime.getInputHandler("CheckStockStream");
        InputHandler updateStockStream = executionPlanRuntime.getInputHandler("UpdateStockStream");
        executionPlanRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 185.6f, 100L});
        checkStockStream.send(new Object[]{"IBM", 150.6f, 100L});
        checkStockStream.send(new Object[]{"WSO2", 175.6f, 100L});
        updateStockStream.send(new Object[]{"IBM", 200f, 100L});
        checkStockStream.send(new Object[]{"IBM", 190.6f, 100L});
        checkStockStream.send(new Object[]{"WSO2", 155.6f, 100L});
        Thread.sleep(2000);

        Assert.assertEquals("Number of success events", 2, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertEquals("Event arrived", true, eventArrived);
        executionPlanRuntime.shutdown();
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

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = executionPlanRuntime.getInputHandler("UpdateStockStream");
            executionPlanRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            updateStockStream.send(new Object[]{"IBM", 57.6f, 100L});
            Thread.sleep(1000);

            Assert.assertEquals("Update failed", 3, getDocCount("*:*", "TEST20"));
            executionPlanRuntime.shutdown();
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

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = executionPlanRuntime.getInputHandler("UpdateStockStream");
            executionPlanRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            updateStockStream.send(new Object[]{"IBM", 57.6f, 100L});
            Thread.sleep(1000);

            Assert.assertEquals("Update failed", 3, getDocCount("*:*", "TEST21"));
            executionPlanRuntime.shutdown();
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
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = executionPlanRuntime.getInputHandler("UpdateStockStream");
        try {
            executionPlanRuntime.start();
            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            updateStockStream.send(new Object[]{"IBM", 57.6f, 100L});
            Thread.sleep(1000);
            SiddhiSolrClient client = indexerService.getSolrServiceClient();
            SolrQuery solrQuery = new SolrQuery("*:*");
            solrQuery.setRows(0);
            long noOfDocs = client.query("TEST11", solrQuery).getResults().getNumFound();
            Assert.assertEquals(3, noOfDocs);
        } finally {
            executionPlanRuntime.shutdown();
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
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = executionPlanRuntime.getInputHandler("UpdateStockStream");
        try {
            executionPlanRuntime.start();
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
            updateStockStream.send(new Object[]{"IBM", 00.6f, 100L});
            Thread.sleep(1000);
            SiddhiSolrClient client = indexerService.getSolrServiceClient();
            SolrQuery solrQuery = new SolrQuery("price:\"0.6\"");
            solrQuery.setRows(0);
            long noOfDocs = client.query("TEST12", solrQuery).getResults().getNumFound();
            Assert.assertEquals(16, noOfDocs);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }
}
