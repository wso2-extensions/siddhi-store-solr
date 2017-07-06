Siddhi-store-solr
======================================
---
##### New version of Siddhi v4.0.0 is built in Java 8.
##### Latest Released Solr Store Version v1.0.2.

This is an extension for siddhi Solr event table implementation. This extension can be used to persist events to a
Solr cloud instance of version 6.x.x.


Features Supported
------------------
 - Defining an Event Table
 - Inserting Events
 - Retrieving Events
 - Deleting Events
 - Updating persisted events
 - Filtering Events
 - Insert or Update Events

---
##### Prerequisites for using the feature
 - A Solr server should be started in cloud mode.
 - A Default configset needs to be uploaded to the zookeper of SolrCloud (If you test in your local machine, you can
 start the Solr cloud using  command "./solr -e cloud" and upload the "basic_configs" configset)

---
##### Deploying the feature
 Feature can be deploy as a OSGI bundle by putting jar file of the component to DAS_HOME/lib directory of DAS 4.0.0 pack.

---
#### Example Siddhi Queries

---
##### Defining an Event Table
 <pre>
 @Store(type="solr", zookeeper.url="localhost:9983", base.configset=<your-default-configset>, collection="SAMPLE_COLLECTION", shards='2', replicas='2', schema ='time long stored, date string stored', commit.async='true')
 @PrimaryKey("symbol")
 define table FooTable (symbol string, price float, volume long);
 </pre>

---
#### Documentation

  * https://docs.wso2.com/display/DAS400/Configuring+Event+Tables+to+Store+Data

---
## How to Contribute

* Please report issues at [Siddhi JIRA] (https://wso2.org/jira/browse/SIDDHI)
* Send your bug fixes pull requests to [master branch] (https://github.com/wso2-extensions/siddhi-io-http/tree/master)

---
## Contact us

Siddhi developers can be contacted via the mailing lists:
  * Carbon Developers List : dev@wso2.org
  * Carbon Architecture List : architecture@wso2.org

---
### We welcome your feedback and contribution.

DAS Team

