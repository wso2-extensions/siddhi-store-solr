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

package org.wso2.extensions.siddhi.store.solr.config;

import org.wso2.extensions.siddhi.store.solr.beans.SolrSchema;
import org.wso2.extensions.siddhi.store.solr.utils.SolrTableUtils;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Represents the Indexing Server details to connect.
 */
@XmlRootElement(name = "indexer-config")
public class CollectionConfiguration {
    private static final String DEFAULT_SOLR_URL = "localhost:9983";
    private static final String BASE_CONFIG_SET = "gettingstarted";
    private static final String DEFAULT_NO_OF_SHARDS = "2";
    private static final String DEFAULT_NO_OF_REPLICA = "1";
    private String solrServerUrl;
    private String collectionName;
    private int noOfShards;
    private int noOfReplicas;
    private String configSet;
    private SolrSchema schema;

    private CollectionConfiguration() {

    }

    private CollectionConfiguration(String collectionName, String solrServerUrl, int noOfShards, int noOfReplicas,
                                    SolrSchema schema, String configSet) {
        this.noOfShards = noOfShards;
        this.noOfReplicas = noOfReplicas;
        this.solrServerUrl = solrServerUrl;
        this.configSet = configSet;
        this.collectionName = collectionName;
        this.schema = schema;
    }

    @XmlElement(name = "solr-cloud-url", defaultValue = DEFAULT_SOLR_URL)
    public String getSolrServerUrl() {
        return solrServerUrl;
    }

    @XmlElement(name = "no-of-shards", defaultValue = DEFAULT_NO_OF_SHARDS)
    public int getNoOfShards() {
        return noOfShards;
    }

    @XmlElement(name = "no-of-replica", defaultValue = DEFAULT_NO_OF_REPLICA)
    public int getNoOfReplicas() {
        return noOfReplicas;
    }

    @XmlElement(name = "default-config-set", defaultValue = BASE_CONFIG_SET)
    public String getConfigSet() {
        return configSet;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public SolrSchema getSchema() {
        return schema;
    }

    /**
     * Builder class for creating the CollectionConfiguration objects.
     */
    public static class Builder {
        private String solrServerUrl;
        private String collectionName;
        private int noOfShards;
        private int noOfReplicas;
        private String configSet;
        private SolrSchema schema;

        public Builder() {

        }

        public Builder solrServerUrl(String solrServerUrl) {

            this.solrServerUrl = SolrTableUtils.normalizeURL(solrServerUrl);
            return this;
        }

        public Builder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Builder shards(int noOfShards) {
            this.noOfShards = noOfShards;
            return this;
        }

        public Builder replicas(int noOfReplicas) {
            this.noOfReplicas = noOfReplicas;
            return this;
        }

        public Builder configs(String configSet) {
            this.configSet = configSet;
            return this;
        }

        public Builder schema(SolrSchema schema) {
            this.schema = schema;
            return this;
        }

        public CollectionConfiguration build() {
            return new CollectionConfiguration(collectionName, solrServerUrl, noOfShards, noOfReplicas, schema,
                                               configSet);
        }
    }
}
