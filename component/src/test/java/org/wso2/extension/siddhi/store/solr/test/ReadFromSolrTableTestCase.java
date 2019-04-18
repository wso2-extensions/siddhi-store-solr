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
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import org.awaitility.Duration;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.store.solr.exceptions.SolrClientServiceException;
import org.wso2.extension.siddhi.store.solr.impl.SolrClientServiceImpl;

import java.util.Arrays;

/**
 * This class contains the tests related to reading from a solr event table
 */
public class ReadFromSolrTableTestCase {

    @Test
    public void readEventsFromSolrEventTable() throws InterruptedException, SolrClientServiceException {
        SiddhiManager siddhiManager = new SiddhiManager();
        SolrClientServiceImpl indexerService = SolrClientServiceImpl.INSTANCE;
        String defineQuery =
                "define stream FooStream (firstname string, lastname string, age int);" +
                "define stream BooStream (firstname string);" +
                "@PrimaryKey('firstname','lastname', 'age')" +
                "@store(type='solr', url='localhost:9983', collection='TEST4', base.config='gettingstarted', " +
                "shards='2', replicas='2', schema='firstname string stored, lastname string stored, age int stored', " +
                "commit.async='true')" +
                "define table FooTable(firstname string, lastname string, age int);";
        String insertQuery = "" +
                             "@info(name = 'query1') " +
                             "from FooStream   " +
                             "insert into FooTable ;";
        String readQuery = "" +
                           "@info(name = 'query2') " +
                           "from BooStream#window.length(1) join FooTable " +
                           "select BooStream.firstname as booname, FooTable.firstname as fooname, FooTable.age" +
                           " as" +
                           " " +
                           "age  " +
                           "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(defineQuery +
                                                                                             insertQuery + readQuery);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    int inEventCount = 0;
                    for (Event event : inEvents) {
                        if (Arrays.equals(new Object[]{"first1", "first1", 23}, event.getData())) {
                            inEventCount++;
                        }
                        if (Arrays.equals(new Object[]{"first1", "first2", 45}, event.getData())) {
                            inEventCount++;
                        }
                        if (Arrays.equals(new Object[]{"first1", "first3", 100}, event.getData())) {
                            inEventCount++;
                        }
                    }
                    Assert.assertEquals(inEventCount, 3);
                    Assert.assertEquals(inEvents.length, 3);
                }
            }

        });
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        InputHandler booStream = siddhiAppRuntime.getInputHandler("BooStream");
        try {
            siddhiAppRuntime.start();
            fooStream.send(new Object[]{"first1", "last1", 23});
            fooStream.send(new Object[]{"first2", "last2", 45});
            fooStream.send(new Object[]{"first3", "last3", 100});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST4", Duration.FIVE_SECONDS);
            booStream.send(new Object[]{"first1"});
        } catch (Exception e) {
            //ignored
        } finally {
            indexerService.deleteCollection("TEST4");
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void read1EventMatchingAValueFromSolrTable() throws InterruptedException, SolrClientServiceException {
        SiddhiManager siddhiManager = new SiddhiManager();
        SolrClientServiceImpl indexerService = SolrClientServiceImpl.INSTANCE;
        String defineQuery =
                "define stream FooStream (firstname string, lastname string, age int);" +
                "define stream BooStream (firstname string);" +
                "@PrimaryKey('firstname','lastname', 'age')" +
                "@store(type='solr', url='localhost:9983', collection='TEST5', base.config='gettingstarted', " +
                "shards='2', replicas='2', schema='firstname string stored, lastname string stored, age int stored', " +
                "commit.async='true')" +
                "define table FooTable(firstname string, lastname string, age int);";
        String insertQuery = "" +
                             "@info(name = 'query1') " +
                             "from FooStream   " +
                             "insert into FooTable ;";
        String readQuery = "" +
                           "@info(name = 'query2') " +
                           "from BooStream#window.length(1) join FooTable " +
                           "select BooStream.firstname as booname, FooTable.lastname as fooname, FooTable.age" +
                           " as" +
                           " " +
                           "age having fooname == 'last2'" +
                           "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(defineQuery +
                                                                                             insertQuery + readQuery);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    Assert.assertEquals(inEvents.length, 1);
                    Assert.assertEquals(inEvents[0].getData(), new Object[]{"first1", "last2", 45});
                }
            }
        });
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        InputHandler booStream = siddhiAppRuntime.getInputHandler("BooStream");
        try {
            siddhiAppRuntime.start();
            fooStream.send(new Object[]{"first1", "last1", 23});
            fooStream.send(new Object[]{"first2", "last2", 45});
            fooStream.send(new Object[]{"first3", "last3", 100});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST5", Duration.FIVE_SECONDS);
            booStream.send(new Object[]{"first1"});
        } catch (Exception e) {
            //ignored
        } finally {
            indexerService.deleteCollection("TEST5");
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void readEventsGreaterThanAValueFromSolrTable() throws InterruptedException, SolrClientServiceException {
        SiddhiManager siddhiManager = new SiddhiManager();
        SolrClientServiceImpl indexerService = SolrClientServiceImpl.INSTANCE;
        String defineQuery =
                "define stream FooStream (firstname string, lastname string, age int);" +
                "define stream BooStream (firstname string);" +
                "@PrimaryKey('firstname','lastname', 'age')" +
                "@store(type='solr', url='localhost:9983', collection='TEST6', base.config='gettingstarted', " +
                "shards='2', replicas='2', schema='firstname string stored, lastname string stored, age int stored', " +
                "commit.async='true')" +
                "define table FooTable(firstname string, lastname string, age int);";
        String insertQuery = "" +
                             "@info(name = 'query1') " +
                             "from FooStream   " +
                             "insert into FooTable ;";
        String readQuery = "" +
                           "@info(name = 'query2') " +
                           "from BooStream#window.length(1) join FooTable " +
                           "select BooStream.firstname as booname, FooTable.lastname as fooname, FooTable.age" +
                           " as" +
                           " " +
                           "age having age > 23" +
                           "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(defineQuery +
                                                                                             insertQuery + readQuery);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                int inEventCount = 0;
                if (inEvents != null) {
                    inEventCount = checkReceivedEvents(inEvents, inEventCount);
                    Assert.assertEquals(inEventCount, 2);
                    Assert.assertEquals(inEvents.length, 2);
                }
            }
        });
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        InputHandler booStream = siddhiAppRuntime.getInputHandler("BooStream");
        try {
            siddhiAppRuntime.start();
            fooStream.send(new Object[]{"first1", "last1", 23});
            fooStream.send(new Object[]{"first2", "last2", 45});
            fooStream.send(new Object[]{"first3", "last3", 100});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST6", Duration.FIVE_SECONDS);
            booStream.send(new Object[]{"first1"});
        } catch (Exception e) {
            //ignored
        } finally {
            indexerService.deleteCollection("TEST6");
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void read1EventGreaterAValueAndMatchingOneValueFromSolrTable()
            throws InterruptedException, SolrClientServiceException {
        SiddhiManager siddhiManager = new SiddhiManager();
        SolrClientServiceImpl indexerService = SolrClientServiceImpl.INSTANCE;
        String defineQuery =
                "define stream FooStream (firstname string, lastname string, age int);" +
                "define stream BooStream (firstname string);" +
                "@PrimaryKey('firstname','lastname', 'age')" +
                "@store(type='solr', url='localhost:9983', collection='TEST7', base.config='gettingstarted', " +
                "shards='2', replicas='2', schema='firstname string stored, lastname string stored, age int stored', " +
                "commit.async='true')" +
                "define table FooTable(firstname string, lastname string, age int);";
        String insertQuery = "" +
                             "@info(name = 'query1') " +
                             "from FooStream   " +
                             "insert into FooTable ;";
        String readQuery = "" +
                           "@info(name = 'query2') " +
                           "from BooStream#window.length(1) join FooTable " +
                           "select BooStream.firstname as booname, FooTable.lastname as fooname, FooTable.age" +
                           " as" +
                           " " +
                           "age having age > 23 and fooname == 'last2'" +
                           "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(defineQuery +
                insertQuery + readQuery);
        checkReceivedEvents(siddhiAppRuntime);
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        InputHandler booStream = siddhiAppRuntime.getInputHandler("BooStream");
        try {
            siddhiAppRuntime.start();
            fooStream.send(new Object[]{"first1", "last1", 23});
            fooStream.send(new Object[]{"first2", "last2", 45});
            fooStream.send(new Object[]{"first3", "last3", 100});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST7", Duration.FIVE_SECONDS);
            booStream.send(new Object[]{"first1"});
        } catch (Exception e) {
            //ignored
        } finally {
            indexerService.deleteCollection("TEST7");
            siddhiAppRuntime.shutdown();
        }
    }

    private void checkReceivedEvents(SiddhiAppRuntime siddhiAppRuntime) {
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    int inEventCount = 0;
                    if (Arrays.equals(new Object[]{"first1", "last2", 45}, inEvents[0].getData())) {
                        inEventCount++;
                    }
                    Assert.assertEquals(inEventCount, 1);
                    Assert.assertEquals(inEvents.length, 1);
                }
            }
        });
    }

    @Test
    public void read1EventMatchingAllFieldsFromSolrTable() throws InterruptedException, SolrClientServiceException {
        SiddhiManager siddhiManager = new SiddhiManager();
        SolrClientServiceImpl indexerService = SolrClientServiceImpl.INSTANCE;
        String defineQuery =
                "define stream FooStream (firstname string, lastname string, age int);" +
                "define stream BooStream (firstname string);" +
                "@PrimaryKey('firstname','lastname', 'age')" +
                "@store(type='solr', url='localhost:9983', collection='TEST8', base.config='gettingstarted', " +
                "shards='2', replicas='2', schema='firstname string stored, lastname string stored, age int stored', " +
                "commit.async='true')" +
                "define table FooTable(firstname string, lastname string, age int);";
        String insertQuery = "" +
                             "@info(name = 'query1') " +
                             "from FooStream   " +
                             "insert into FooTable ;";
        String readQuery = "" +
                           "@info(name = 'query2') " +
                           "from BooStream#window.length(1) join FooTable " +
                           "select BooStream.firstname as booname, FooTable.lastname as fooname, FooTable.age" +
                           " as" +
                           " " +
                           "age having age == 45 and fooname == 'last2' and booname == 'first1'" +
                           "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(defineQuery +
                insertQuery + readQuery);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                int inEventCount = 0;
                if (inEvents != null) {
                    if (Arrays.equals(new Object[]{"first1", "last2", 45}, inEvents[0].getData())) {
                        inEventCount++;
                    }
                    Assert.assertEquals(inEventCount, 1);
                    Assert.assertEquals(inEvents.length, 1);
                }
            }
        });
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        InputHandler booStream = siddhiAppRuntime.getInputHandler("BooStream");
        try {
            siddhiAppRuntime.start();
            fooStream.send(new Object[]{"first1", "last1", 23});
            fooStream.send(new Object[]{"first2", "last2", 45});
            fooStream.send(new Object[]{"first3", "last3", 100});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST8", Duration.FIVE_SECONDS);
            booStream.send(new Object[]{"first1"});
        } catch (Exception e) {
            //ignored
        } finally {
            indexerService.deleteCollection("TEST8");
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void read1EventWithMultipleOperatorsFromSolrTable() throws InterruptedException, SolrClientServiceException {
        SiddhiManager siddhiManager = new SiddhiManager();
        SolrClientServiceImpl indexerService = SolrClientServiceImpl.INSTANCE;
        String defineQuery =
                "define stream FooStream (firstname string, lastname string, age int);" +
                "define stream BooStream (firstname string);" +
                "@PrimaryKey('firstname','lastname', 'age')" +
                "@store(type='solr', url='localhost:9983', collection='TEST9', base.config='gettingstarted', " +
                "shards='2', replicas='2', schema='firstname string stored, lastname string stored, age int stored', " +
                "commit.async='true')" +
                "define table FooTable(firstname string, lastname string, age int);";
        String insertQuery = "" +
                             "@info(name = 'query1') " +
                             "from FooStream   " +
                             "insert into FooTable ;";
        String readQuery = "" +
                           "@info(name = 'query2') " +
                           "from BooStream#window.length(1) join FooTable " +
                           "select BooStream.firstname as booname, FooTable.lastname as fooname, FooTable.age" +
                           " as" +
                           " " +
                           "age having age == 45 and fooname == 'last2' and booname == 'first1'" +
                           "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(defineQuery +
                                                                                             insertQuery + readQuery);
        checkReceivedEvents(siddhiAppRuntime);
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        InputHandler booStream = siddhiAppRuntime.getInputHandler("BooStream");
        try {
            siddhiAppRuntime.start();
            fooStream.send(new Object[]{"first1", "last1", 23});
            fooStream.send(new Object[]{"first2", "last2", 45});
            fooStream.send(new Object[]{"first3", "last3", 100});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST9", Duration.FIVE_SECONDS);
            booStream.send(new Object[]{"first1"});
        } catch (Exception e) {
            //ignored
        } finally {
            indexerService.deleteCollection("TEST9");
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void readEventsWithMultipleOperatorsFromSolrTable() throws InterruptedException, SolrClientServiceException {
        SiddhiManager siddhiManager = new SiddhiManager();
        SolrClientServiceImpl indexerService = SolrClientServiceImpl.INSTANCE;
        String defineQuery =
                "define stream FooStream (firstname string, lastname string, age int);" +
                "define stream BooStream (firstname string);" +
                "@PrimaryKey('firstname','lastname', 'age')" +
                "@store(type='solr', url='localhost:9983', collection='TEST10', base.config='gettingstarted', " +
                "shards='2', replicas='2', schema='firstname string stored, lastname string stored, age int stored', " +
                "commit.async='true')" +
                "define table FooTable(firstname string, lastname string, age int);";
        String insertQuery = "" +
                             "@info(name = 'query1') " +
                             "from FooStream   " +
                             "insert into FooTable ;";
        String readQuery = "" +
                           "@info(name = 'query2') " +
                           "from BooStream#window.length(1) join FooTable " +
                           "select BooStream.firstname as booname, FooTable.lastname as fooname, FooTable.age" +
                           " as" +
                           " " +
                           "age having (age == 45 and fooname == 'last2') or age == 100" +
                           "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(defineQuery +
                                                                                             insertQuery + readQuery);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    int inEventCount = 0;
                    inEventCount = checkReceivedEvents(inEvents, inEventCount);
                    Assert.assertEquals(inEventCount, 2);
                    Assert.assertEquals(inEvents.length, 2);

                }
            }
        });
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        InputHandler booStream = siddhiAppRuntime.getInputHandler("BooStream");
        try {
            siddhiAppRuntime.start();
            fooStream.send(new Object[]{"first1", "last1", 23});
            fooStream.send(new Object[]{"first2", "last2", 45});
            fooStream.send(new Object[]{"first3", "last3", 100});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST10", Duration.FIVE_SECONDS);
            booStream.send(new Object[]{"first1"});
        } catch (Exception e) {
            //ignored
        } finally {
            indexerService.deleteCollection("TEST10");
            siddhiAppRuntime.shutdown();
        }
    }

    private int checkReceivedEvents(Event[] inEvents, int inEventCount) {
        for (Event event : inEvents) {
            if (Arrays.equals(new Object[]{"first1", "last2", 45}, event.getData())) {
                inEventCount++;
            }
            if (Arrays.equals(new Object[]{"first1", "last3", 100}, event.getData())) {
                inEventCount++;
            }
        }
        return inEventCount;
    }

    @Test
    public void readEventsWithComplexConditionFromSolrTable() throws InterruptedException, SolrClientServiceException {
        SiddhiManager siddhiManager = new SiddhiManager();
        SolrClientServiceImpl indexerService = SolrClientServiceImpl.INSTANCE;
        String defineQuery =
                "define stream FooStream (firstname string, lastname string, age int, school string, index int);" +
                "define stream BooStream (firstname string);" +
                "@PrimaryKey('index')" +
                "@store(type='solr', url='localhost:9983', collection='TEST11', base.config='gettingstarted', " +
                "shards='2', replicas='2', schema='firstname string stored, lastname string stored, age int stored, " +
                "school string stored, index int" +
                "', " +
                "commit.async='true')" +
                "define table FooTable(firstname string, lastname string, age int, school string, index int);";
        String insertQuery = "" +
                             "@info(name = 'query1') " +
                             "from FooStream   " +
                             "insert into FooTable ;";
        String readQuery = "" +
                           "@info(name = 'query2') " +
                           "from BooStream#window.length(1) join FooTable " +
                           "select FooTable.firstname as booname, FooTable.lastname as fooname, FooTable.age" +
                           " as age, FooTable.school as school, FooTable.index as index " +
                           "having ((age == 45 or fooname == 'last1') and (school == 'school2' or index == 1006)) or " +
                           "((age == 56 or fooname == 'last1') and (school == 'school2' or index == 1006))" +
                           "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(defineQuery +
                                                                                             insertQuery + readQuery);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    int inEventCount = 0;
                    for (Event event : inEvents) {
                        if (Arrays.equals(new Object[]{"first2", "last2", 45, "school2", 1002}, event.getData())) {
                            inEventCount++;
                        }
                        if (Arrays.equals(new Object[]{"first6", "last6", 56, "school6", 1006}, event.getData())) {
                            inEventCount++;
                        }
                    }
                    Assert.assertEquals(inEventCount, 2);
                    Assert.assertEquals(inEvents.length, 2);

                }
            }
        });
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        InputHandler booStream = siddhiAppRuntime.getInputHandler("BooStream");
        try {
            siddhiAppRuntime.start();
            fooStream.send(new Object[]{"first1", "last1", 23, "school1", 1001});
            fooStream.send(new Object[]{"first2", "last2", 45, "school2", 1002});
            fooStream.send(new Object[]{"first3", "last3", 100, "school3", 1003});
            fooStream.send(new Object[]{"first4", "last4", 56, "school4", 1004});
            fooStream.send(new Object[]{"first5", "last5", 43, "school5", 1005});
            fooStream.send(new Object[]{"first6", "last6", 56, "school6", 1006});
            SolrTestUtils.waitTillEventsPersist(indexerService, 3, "TEST11", Duration.FIVE_SECONDS);
            booStream.send(new Object[]{"first1"});
        } catch (Exception e) {
            //ignored
        } finally {
            indexerService.deleteCollection("TEST11");
            siddhiAppRuntime.shutdown();
        }
    }
}
