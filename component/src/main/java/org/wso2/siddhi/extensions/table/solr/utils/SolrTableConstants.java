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

package org.wso2.siddhi.extensions.table.solr.utils;

/**
 * This class contains the constants values required by the solr table implementation
 */
public class SolrTableConstants {

    public static final String ANNOTATION_ELEMENT_COLLECTION = "collection";
    public static final String ANNOTATION_ELEMENT_URL = "zookeeper.url";
    public static final String ANNOTATION_ELEMENT_SHARDS = "shards";
    public static final String ANNOTATION_ELEMENT_REPLICAS = "replicas";
    public static final String ANNOTATION_ELEMENT_SCHEMA = "schema";
    public static final String ANNOTATION_ELEMENT_CONFIGSET = "base.config";
    public static final String ANNOTATION_ELEMENT_COMMIT_ASYNC = "commit.async";
    public static final String ANNOTATION_ELEMENT_MERGE_SCHEMA = "merge.schema";

    public static final String PROPERTY_READ_BATCH_SIZE = "read.batch.size";

    public static final String DEFAULT_ZOOKEEPER_URL = "localhost:9983";
    public static final String DEFAULT_SHARD_COUNT  = "2";
    public static final String DEFAULT_REPLICAS_COUNT  = "1";
    public static final String DEFAULT_READ_ITERATOR_BATCH_SIZE = "1000";

}
