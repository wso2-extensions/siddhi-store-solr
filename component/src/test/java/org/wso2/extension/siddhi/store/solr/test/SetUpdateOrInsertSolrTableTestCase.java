/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extension.siddhi.store.solr.test;

import org.apache.solr.client.solrj.SolrServerException;
import org.awaitility.Duration;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.store.solr.exceptions.SolrClientServiceException;
import org.wso2.extension.siddhi.store.solr.impl.SolrClientServiceImpl;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.io.IOException;

public class SetUpdateOrInsertSolrTableTestCase {
    private static SolrClientServiceImpl indexerService;

    @BeforeClass
    public static void startTest() {
        indexerService = SolrClientServiceImpl.INSTANCE;
    }

    @Test(testName = "setUpdateOrInsertSolrTableTestCase1", description = "setting all columns")
    public void setUpdateOrInsertSolrTableTestCase1()
            throws InterruptedException, SolrClientServiceException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                         "define stream StockStream (symbol string, price float, volume long); " +
                         "define stream UpdateStockStream (symbol string, price float, volume long); " +
                         "@Store(type='solr', url='localhost:9983', collection='TEST44', base" +
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
                       "update or insert into StockTable " +
                       "set StockTable.price = price, StockTable.symbol = symbol, " +
                       "   StockTable.volume = volume  " +
                       "   on StockTable.symbol == symbol ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        try {
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST44", Duration.FIVE_SECONDS);
            updateStockStream.send(new Object[]{"IBM", 100f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, "price:\"100.0\"", 1, "TEST44",
                                                Duration.FIVE_SECONDS);
            Assert.assertEquals(SolrTestUtils.getDocCount(indexerService, "TEST44"), 3, "Update failed");
        } catch (Exception e) {
            Assert.fail();
        } finally {
            indexerService.deleteCollection("TEST44");
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(testName = "setUpdateOrInsertSolrTableTestCase2", description = "setting a subset of columns")
    public void setUpdateOrInsertSolrTableTestCase2()
            throws InterruptedException, SolrServerException, SolrClientServiceException, IOException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                         "define stream StockStream (symbol string, price float, volume long); " +
                         "define stream UpdateStockStream (symbol string, price float, volume long); " +
                         "@Store(type='solr', url='localhost:9983', collection='TEST45', base" +
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
                       "update or insert into StockTable " +
                       "set StockTable.price = price, StockTable.symbol = symbol " +
                       "   on StockTable.symbol == symbol ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        try {
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST45", Duration.FIVE_SECONDS);
            updateStockStream.send(new Object[]{"IBM", 100f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, "price:\"100.0\"", 1, "TEST45",
                                                Duration.FIVE_SECONDS);
            Assert.assertEquals(SolrTestUtils.getDocCount(indexerService, "TEST45"), 3, "Update failed");
        } finally {
            indexerService.deleteCollection("TEST45");
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(testName = "setUpdateOrInsertSolrTableTestCase3",
            description = "using a constant value as the assigment expression")
    public void setUpdateOrInsertSolrTableTestCase3()
            throws InterruptedException, SolrServerException, IOException, SolrClientServiceException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                         "define stream StockStream (symbol string, price float, volume long); " +
                         "define stream UpdateStockStream (symbol string, price float, volume long); " +
                         "@Store(type='solr', url='localhost:9983', collection='TEST46', base" +
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
                       "update or insert into StockTable " +
                       "set StockTable.price = 10 " +
                       "   on StockTable.symbol == symbol ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        try {
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST46", Duration.FIVE_SECONDS);
            updateStockStream.send(new Object[]{"IBM", 100f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, "price:\"10.0\"", 1, "TEST46",
                                                Duration.FIVE_SECONDS);
            Assert.assertEquals(SolrTestUtils.getDocCount(indexerService, "TEST46"), 3, "Update failed");
        } finally {
            indexerService.deleteCollection("TEST46");
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(testName = "setUpdateOrInsertSolrTableTestCase4",
            description = "using one of the output attribute values in the " +
                          "select clause as the assignment expression.", enabled = false)
    public void setUpdateOrInsertSolrTableTestCase4()
            throws InterruptedException, SolrServerException, IOException, SolrClientServiceException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                         "define stream StockStream (symbol string, price float, volume long); " +
                         "define stream UpdateStockStream (symbol string, price float, volume long); " +
                         "@Store(type='solr', url='localhost:9983', collection='TEST47', base" +
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
//                    "select price, symbol, volume " +
                       "update or insert into StockTable " +
                       "set StockTable.price = price + 100  " +
                       "   on StockTable.symbol == symbol ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        try {
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST47", Duration.FIVE_SECONDS);
            updateStockStream.send(new Object[]{"IBM", 100f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, "price:\"100.0\"", 1, "TEST47",
                                                Duration.FIVE_SECONDS);
            Assert.assertEquals(SolrTestUtils.getDocCount(indexerService, "TEST47"), 3, "Update failed");
        } finally {
            indexerService.deleteCollection("TEST47");
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(testName = "setUpdateOrInsertSolrTableTestCase5",
            description = "assignment expression containing an output attribute", enabled = false)
    public void setUpdateOrInsertSolrTableTestCase5()
            throws InterruptedException, SolrServerException, IOException, SolrClientServiceException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                         "define stream StockStream (symbol string, price float, volume long); " +
                         "define stream UpdateStockStream (symbol string, price float, volume long); " +
                         "@Store(type='solr', url='localhost:9983', collection='TEST48', base" +
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
                       "update or insert into StockTable " +
                       "set StockTable.price = price + 100 " +
                       "   on StockTable.symbol == symbol ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        try {
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST48", Duration.FIVE_SECONDS);
            updateStockStream.send(new Object[]{"IBM", 100f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, "price:\"100.0\"", 1, "TEST48",
                                                Duration.FIVE_SECONDS);
            Assert.assertEquals(SolrTestUtils.getDocCount(indexerService, "TEST48"), 3, "Update failed");
        } finally {
            indexerService.deleteCollection("TEST48");
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(testName = "setUpdateOrInsertSolrTableTestCase6",
            description = "Omitting table name from the LHS of set assignment")
    public void setUpdateOrInsertSolrTableTestCase6()
            throws InterruptedException, SolrServerException, IOException, SolrClientServiceException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                         "define stream StockStream (symbol string, price float, volume long); " +
                         "define stream UpdateStockStream (symbol string, price float, volume long); " +
                         "@Store(type='solr', url='localhost:9983', collection='TEST49', base" +
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
                       "update or insert into StockTable " +
                       "set price = 100 " +
                       "   on StockTable.symbol == symbol ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        try {
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST49", Duration.FIVE_SECONDS);
            updateStockStream.send(new Object[]{"IBM", 100f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, "price:\"100.0\"", 1, "TEST49",
                                                Duration.FIVE_SECONDS);
            Assert.assertEquals(SolrTestUtils.getDocCount(indexerService, "TEST49"), 3, "Update failed");
        } finally {
            indexerService.deleteCollection("TEST49");
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(testName = "setUpdateOrInsertSolrTableTestCase7",
            description = "Set clause should be optional")
    public void setUpdateOrInsertSolrTableTestCase7()
            throws InterruptedException, SolrServerException, IOException, SolrClientServiceException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                         "define stream StockStream (symbol string, price float, volume long); " +
                         "define stream UpdateStockStream (symbol string, price float, volume long); " +
                         "@Store(type='solr', url='localhost:9983', collection='TEST50', base" +
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
                       "update or insert into StockTable " +
                       "   on StockTable.symbol == symbol ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        try {
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST50", Duration.FIVE_SECONDS);
            updateStockStream.send(new Object[]{"IBM", 100f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, "price:\"100.0\"", 1, "TEST50",
                                                Duration.FIVE_SECONDS);
            Assert.assertEquals(SolrTestUtils.getDocCount(indexerService, "TEST50"), 3, "Update failed");
        } finally {
            indexerService.deleteCollection("TEST50");
            siddhiAppRuntime.shutdown();
        }
    }
}
