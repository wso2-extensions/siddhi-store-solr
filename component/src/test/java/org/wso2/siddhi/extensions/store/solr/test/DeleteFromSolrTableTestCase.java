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
import org.junit.BeforeClass;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.extensions.store.solr.exceptions.SolrClientServiceException;
import org.wso2.siddhi.extensions.store.solr.impl.SiddhiSolrClient;
import org.wso2.siddhi.extensions.store.solr.impl.SolrClientServiceImpl;

import java.io.IOException;

/**
 * This class represents the tests related to delete queries in Solr Store implementation
 */
public class DeleteFromSolrTableTestCase {
    private static final Log log = LogFactory.getLog(DeleteFromSolrTableTestCase.class);
    private static SolrClientServiceImpl indexerService;

    @BeforeClass
    public static void startTest() {
        log.info("== Solr Table DELETE tests started ==");
        indexerService = SolrClientServiceImpl.getInstance();
    }

    @AfterClass
    public static void shutdown() throws SolrClientServiceException {
        if (indexerService == null) {
            throw new SolrClientServiceException("Indexer Service cannot be loaded!");
        }
        try {
            indexerService.deleteCollection("TEST34");
            indexerService.deleteCollection("TEST35");
            indexerService.deleteCollection("TEST36");
            indexerService.deleteCollection("TEST37");
            indexerService.deleteCollection("TEST38");
            indexerService.deleteCollection("TEST39");
            indexerService.deleteCollection("TEST40");
            indexerService.deleteCollection("TEST41");
            indexerService.deleteCollection("TEST42");
            indexerService.deleteCollection("TEST43");
        } finally {
            indexerService.destroy();
        }
        log.info("== Solr Table DELETE tests completed ==");
    }

    private long getDocCount(String query, String collection)
            throws SolrClientServiceException, IOException, SolrServerException {
        SiddhiSolrClient client;
        client = indexerService.getSolrServiceClient();
        SolrQuery solrQuery = new SolrQuery(query);
        solrQuery.setRows(0);
        return client.query(collection, solrQuery).getResults().getNumFound();
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

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = executionPlanRuntime.getInputHandler("DeleteStockStream");
            executionPlanRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"IBM", 75.6F, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
            deleteStockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            Thread.sleep(1000);

            Assert.assertEquals("Deletion failed", 0, getDocCount("*:*", "TEST34"));

            executionPlanRuntime.shutdown();
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

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = executionPlanRuntime.getInputHandler("DeleteStockStream");
            executionPlanRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"IBM", 75.6F, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
            deleteStockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            Thread.sleep(1000);

            Assert.assertEquals("Deletion failed", 0, getDocCount("*:*", "TEST35"));
            executionPlanRuntime.shutdown();
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

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = executionPlanRuntime.getInputHandler("DeleteStockStream");
            executionPlanRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"IBM", 75.6F, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
            Thread.sleep(1000);

            Assert.assertEquals("Deletion failed", 2, getDocCount("*:*", "TEST36"));
            executionPlanRuntime.shutdown();
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

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = executionPlanRuntime.getInputHandler("DeleteStockStream");
            executionPlanRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"IBM", 75.6F, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
            Thread.sleep(1000);

            Assert.assertEquals("Deletion failed", 2, getDocCount("*:*", "TEST37"));
            executionPlanRuntime.shutdown();
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
                           "   on 'IBM' == symbol  ;"; //TODO symbol is ambiguous

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = executionPlanRuntime.getInputHandler("DeleteStockStream");
            executionPlanRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"IBM", 75.6F, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            deleteStockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            Thread.sleep(1000);

            Assert.assertEquals("Deletion failed", 2, getDocCount("*:*", "TEST38"));
            executionPlanRuntime.shutdown();
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
                           "   on symbol == 'IBM'  ;"; //TODO seems symbol is ambiguous

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = executionPlanRuntime.getInputHandler("DeleteStockStream");
            executionPlanRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"IBM", 75.6F, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
            Thread.sleep(1000);

            Assert.assertEquals("Deletion failed", 2, getDocCount("*:*", "TEST39"));
            executionPlanRuntime.shutdown();
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

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = executionPlanRuntime.getInputHandler("DeleteStockStream");
            executionPlanRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"IBM", 75.6F, 100L});
            stockStream.send(new Object[]{"IBM", 57.6F, 100L});
            deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
            Thread.sleep(1000);

            Assert.assertEquals("Deletion failed", 2, getDocCount("*:*", "TEST40"));
            executionPlanRuntime.shutdown();
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

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = executionPlanRuntime.getInputHandler("DeleteStockStream");
            executionPlanRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"IBM", 75.6F, 100L});
            stockStream.send(new Object[]{"IBM", 57.6F, 100L});
            deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
            Thread.sleep(1000);

            Assert.assertEquals("Deletion failed", 1, getDocCount("*:*", "TEST41"));
            executionPlanRuntime.shutdown();
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

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = executionPlanRuntime.getInputHandler("DeleteStockStream");
            executionPlanRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"IBM", 75.6F, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
            Thread.sleep(1000);

            Assert.assertEquals("Deletion failed", 2, getDocCount("*:*", "TEST42"));
            executionPlanRuntime.shutdown();
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

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = executionPlanRuntime.getInputHandler("DeleteStockStream");
            executionPlanRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"IBM", 75.6F, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
            Thread.sleep(1000);

            Assert.assertEquals("Deletion failed", 2, getDocCount("*:*", "TEST43"));
            Thread.sleep(1000);

            stockStream.send(new Object[]{null, 45.5F, 100L});
            executionPlanRuntime.shutdown();
            Thread.sleep(1000);
            try {
                siddhiManager.createExecutionPlanRuntime(streams + query);
            } catch (NullPointerException ex) {
                Assert.fail("Cannot Process null values in bloom filter");
            }
        } catch (Exception e) {
            log.info("Test case 'deleteFromSolrTableTest11' ignored due to " + e.getMessage(), e);
        }
    }
}
