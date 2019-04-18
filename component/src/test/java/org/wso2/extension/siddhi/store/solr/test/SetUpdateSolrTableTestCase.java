/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extension.siddhi.store.solr.test;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.stream.input.InputHandler;
import org.apache.solr.client.solrj.SolrServerException;
import org.awaitility.Duration;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.store.solr.exceptions.SolrClientServiceException;
import org.wso2.extension.siddhi.store.solr.impl.SolrClientServiceImpl;

import java.io.IOException;
import java.sql.SQLException;


public class SetUpdateSolrTableTestCase {
    private static SolrClientServiceImpl indexerService;

    @BeforeClass
    public static void startTest() {
        indexerService = SolrClientServiceImpl.INSTANCE;
    }

    @Test(testName = "setUpdateSolrTableTestCase1", description = "setting all columns")
    public void setUpdateSolrTableTestCase1()
            throws InterruptedException, SQLException, SolrServerException, IOException, SolrClientServiceException {
        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                         "define stream StockStream (symbol string, price float, volume long); " +
                         "define stream UpdateStockStream (symbol string, price float, volume long); " +
                         "@Store(type='solr', url='localhost:9983', collection='TEST58', base" +
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
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST58", Duration.FIVE_SECONDS);
            updateStockStream.send(new Object[]{"IBM", 100f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, "price:\"100.0\"", 1, "TEST58",
                                                Duration.FIVE_SECONDS);
            Assert.assertEquals(SolrTestUtils.getDocCount(indexerService, "TEST58"), 3, "Update failed");
        } finally {
            indexerService.deleteCollection("TEST58");
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(testName = "setUpdateSolrTableTestCase2", description = "setting a subset of columns")
    public void setUpdateSolrTableTestCase2()
            throws InterruptedException, SQLException, SolrServerException, IOException, SolrClientServiceException {
        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                         "define stream StockStream (symbol string, price float, volume long); " +
                         "define stream UpdateStockStream (symbol string, price float, volume long); " +
                         "@Store(type='solr', url='localhost:9983', collection='TEST57', base" +
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
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST57", Duration.FIVE_SECONDS);
            updateStockStream.send(new Object[]{"IBM", 100f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, "price:\"100.0\"", 1, "TEST57",
                                                Duration.FIVE_SECONDS);
            Assert.assertEquals(SolrTestUtils.getDocCount(indexerService, "TEST57"), 3, "Update failed");
        } finally {
            indexerService.deleteCollection("TEST57");
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(testName = "setUpdateSolrTableTestCase3",
            description = "using a constant value as the assigment expression")
    public void setUpdateSolrTableTestCase3()
            throws InterruptedException, SQLException, SolrServerException, IOException, SolrClientServiceException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                         "define stream StockStream (symbol string, price float, volume long); " +
                         "define stream UpdateStockStream (symbol string, price float, volume long); " +
                         "@Store(type='solr', url='localhost:9983', collection='TEST56', base" +
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
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST56", Duration.FIVE_SECONDS);
            updateStockStream.send(new Object[]{"IBM", 100f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, "price:\"10.0\"", 1, "TEST56",
                                                Duration.FIVE_SECONDS);
            Assert.assertEquals(SolrTestUtils.getDocCount(indexerService, "TEST56"), 3, "Update failed");
        } finally {
            indexerService.deleteCollection("TEST56");
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(testName = "setUpdateSolrTableTestCase4",
            description = "using one of the output attribute values in the " +
                          "select clause as the assignment expression.")
    public void setUpdateSolrTableTestCase4()
            throws InterruptedException, SQLException, SolrServerException, IOException, SolrClientServiceException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                         "define stream StockStream (symbol string, price float, volume long); " +
                         "define stream UpdateStockStream (symbol string, price float, volume long); " +
                         "@Store(type='solr', url='localhost:9983', collection='TEST55', base" +
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
                       "select price + 100 as newPrice , symbol " +
                       "update StockTable " +
                       "set StockTable.price = newPrice " +
                       "   on StockTable.symbol == symbol ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        try {
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST55", Duration.FIVE_SECONDS);
            updateStockStream.send(new Object[]{"IBM", 100f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, "price:\"200.0\"", 1, "TEST55",
                                                Duration.FIVE_SECONDS);
            Assert.assertEquals(SolrTestUtils.getDocCount(indexerService, "TEST55"), 3, "Update failed");
        } finally {
            indexerService.deleteCollection("TEST55");
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(testName = "setUpdateSolrTableTestCase5",
            description = "assignment expression containing an output attribute " +
                          "with a basic arithmatic operation.", enabled = false)
    public void setUpdateSolrTableTestCase5()
            throws InterruptedException, SQLException, SolrServerException, IOException, SolrClientServiceException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                         "define stream StockStream (symbol string, price float, volume long); " +
                         "define stream UpdateStockStream (symbol string, price float, volume long); " +
                         "@Store(type='solr', url='localhost:9983', collection='TEST54', base" +
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
                       "select price + 100 as newPrice , symbol " +
                       "update StockTable " +
                       "set StockTable.price = newPrice + 100 " +
                       "   on StockTable.symbol == symbol ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        try {
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST54", Duration.FIVE_SECONDS);
            updateStockStream.send(new Object[]{"IBM", 100f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, "price:\"100.0\"", 1, "TEST54",
                                                Duration.FIVE_SECONDS);
            Assert.assertEquals(SolrTestUtils.getDocCount(indexerService, "TEST54"), 3, "Update failed");
        } finally {
            indexerService.deleteCollection("TEST54");
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(testName = "setUpdateSolrTableTestCase6",
            description = "Omitting table name from the LHS of set assignment")
    public void setUpdateSolrTableTestCase6()
            throws InterruptedException, SQLException, SolrServerException, IOException, SolrClientServiceException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                         "define stream StockStream (symbol string, price float, volume long); " +
                         "define stream UpdateStockStream (symbol string, price float, volume long); " +
                         "@Store(type='solr', url='localhost:9983', collection='TEST53', base" +
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
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST53", Duration.FIVE_SECONDS);
            updateStockStream.send(new Object[]{"IBM", 100f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, "price:\"100.0\"", 1, "TEST53",
                                                Duration.FIVE_SECONDS);
            Assert.assertEquals(SolrTestUtils.getDocCount(indexerService, "TEST53"), 3, "Update failed");
        } finally {
            indexerService.deleteCollection("TEST53");
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(testName = "setUpdateSolrTableTestCase7",
            description = "Set clause should be optional")
    public void setUpdateSolrTableTestCase7()
            throws InterruptedException, SQLException, SolrServerException, IOException, SolrClientServiceException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                         "define stream StockStream (symbol string, price float, volume long); " +
                         "define stream UpdateStockStream (symbol string, price float, volume long); " +
                         "@Store(type='solr', url='localhost:9983', collection='TEST52', base" +
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
        try {
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST52", Duration.FIVE_SECONDS);
            updateStockStream.send(new Object[]{"IBM", 100f, 100L});
            SolrTestUtils.waitTillEventsPersist(indexerService, "price:\"100.0\"", 1, "TEST52",
                                                Duration.FIVE_SECONDS);
            Assert.assertEquals(SolrTestUtils.getDocCount(indexerService, "TEST52"), 3, "Update failed");
        } finally {
            indexerService.deleteCollection("TEST52");
            siddhiAppRuntime.shutdown();
        }
    }
}
