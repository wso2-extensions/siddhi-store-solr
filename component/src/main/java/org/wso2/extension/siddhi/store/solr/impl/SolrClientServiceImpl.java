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

package org.wso2.extension.siddhi.store.solr.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.ConfigSetAdminResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.SolrException;
import org.wso2.extension.siddhi.store.solr.beans.SiddhiSolrDocument;
import org.wso2.extension.siddhi.store.solr.beans.SolrSchema;
import org.wso2.extension.siddhi.store.solr.beans.SolrSchemaField;
import org.wso2.extension.siddhi.store.solr.config.CollectionConfiguration;
import org.wso2.extension.siddhi.store.solr.exceptions.SolrClientServiceException;
import org.wso2.extension.siddhi.store.solr.exceptions.SolrSchemaNotFoundException;
import org.wso2.extension.siddhi.store.solr.utils.SolrTableUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * This class represents the client service which interact with the solr cloud
 */
public enum  SolrClientServiceImpl {

    INSTANCE;
    private static final String ATTR_ERRORS = "errors";
    private static final String ATTR_COLLECTIONS = "collections";
    private static Log log = LogFactory.getLog(SolrClientServiceImpl.class);
    private volatile SiddhiSolrClient indexerClient = null;
    private static Map<String, CollectionConfiguration> tableToConfigMapping = new ConcurrentHashMap<>();
    private static Map<String, SiddhiSolrClient> urlToSolrClientMapping = new ConcurrentHashMap<>();
    private static Map<String, Integer> urlToTableCountMapping = new ConcurrentHashMap<>();

    public SiddhiSolrClient getSolrServiceClientByCollection(String collection) throws SolrClientServiceException {
        synchronized (this) {
            CollectionConfiguration config = tableToConfigMapping.get(collection);
            if (config == null) {
                throw new SolrClientServiceException("No Solr collection definition found for collection: " +
                                                     collection);
            }
            String solrServerURL = config.getSolrServerUrl();
            return getSolrServiceClientByURL(solrServerURL);
        }
    }

    public SiddhiSolrClient getSolrServiceClientByURL(String url) throws SolrClientServiceException {
        String solrServerURL = SolrTableUtils.normalizeURL(url);
        SiddhiSolrClient client = urlToSolrClientMapping.get(solrServerURL);
        if (client == null) {
            throw new SolrClientServiceException("No SolrClient found for ZooKeeper URL: " + solrServerURL);
        }
        return client;
    }

    public boolean initCollection(CollectionConfiguration config)
            throws SolrClientServiceException {
        String table = config.getCollectionName();
        String tableNameWithDomain = SolrTableUtils.getCollectionNameWithDomainName(config.getDomainName(), table);
        initSolrClientForTable(config);
        try {
            if (!collectionExists(table)) {
                if (!collectionConfigExists(table)) {
                    ConfigSetAdminResponse configSetResponse = createInitialSolrCollectionConfig(config);
                    Object errors = configSetResponse.getErrorMessages();
                    if (configSetResponse.getStatus() == 0 && errors == null) {
                        return createSolrCollection(tableNameWithDomain, config);
                    } else {
                        throw new SolrClientServiceException("Error in deploying initial solr configset for " +
                                "table: " + tableNameWithDomain + ", Response code: " + configSetResponse.getStatus() +
                                " , errors: " + errors.toString());
                    }
                } else {
                    return createSolrCollection(tableNameWithDomain, config);
                }
            } else {
                return false;
            }
        } catch (SolrServerException | IOException | SolrException e) {
            throw new SolrClientServiceException("error while creating the index for table: " + table + " error: " +
                    e.getMessage(), e);
        }
    }

    private void initSolrClientForTable(CollectionConfiguration config) throws SolrClientServiceException {
        synchronized (this) {
            String serverURL = config.getSolrServerUrl();
            if (serverURL == null || serverURL.isEmpty()) {
                throw new SolrClientServiceException("Solr server URL for collection: " + config.getCollectionName() +
                                                     " cannot be empty or null");
            }
            tableToConfigMapping.put(config.getCollectionName(), config);
            CloudSolrClient solrClient = new CloudSolrClient.Builder().withZkHost(config.getSolrServerUrl()).build();
            urlToSolrClientMapping.put(config.getSolrServerUrl(), new SiddhiSolrClient(config.getDomainName(),
                    solrClient));
            Integer count = urlToTableCountMapping.get(config.getSolrServerUrl());
            if (count == null) {
                urlToTableCountMapping.put(config.getSolrServerUrl(), Integer.valueOf(1));
            } else {
                urlToTableCountMapping.put(config.getSolrServerUrl(), count + 1);
            }
        }
    }

    /*
    This method is to create the initial index configurations for the index of a table. This will include a default
    indexSchema and other Solr configurations. Later by using updateSolrSchema we can edit the index schema
    */
    private ConfigSetAdminResponse createInitialSolrCollectionConfig(CollectionConfiguration config)
            throws SolrServerException, IOException,
                   SolrClientServiceException {
        String tableNameWithDomain = SolrTableUtils.getCollectionNameWithDomainName(config.getDomainName(), config
                .getCollectionName());
        ConfigSetAdminRequest.Create configSetAdminRequest = new ConfigSetAdminRequest.Create();
        if (config.getConfigSet() != null && !config.getConfigSet().trim().isEmpty()) {
            configSetAdminRequest.setBaseConfigSetName(config.getConfigSet());
        } else {
            throw new SolrClientServiceException("Base configset cannot be found");
        }
        configSetAdminRequest.setConfigSetName(tableNameWithDomain);
        return configSetAdminRequest.process(getSolrServiceClientByCollection(config.getCollectionName()));
    }

    private boolean createSolrCollection(String tableNameWithDomain, CollectionConfiguration config)
            throws SolrServerException, IOException, SolrClientServiceException {
        CollectionAdminRequest.Create createRequest =
                CollectionAdminRequest.createCollection(tableNameWithDomain, tableNameWithDomain,
                                                        config.getNoOfShards(),
                                                        config.getNoOfReplicas());
        createRequest.setMaxShardsPerNode(config.getNoOfShards());
        CollectionAdminResponse collectionAdminResponse =
                createRequest.process(getSolrServiceClientByCollection(config.getCollectionName()));
        if (!collectionAdminResponse.isSuccess()) {
            Object errors = collectionAdminResponse.getErrorMessages();
            throw new SolrClientServiceException("Error in deploying initial solr configset for collection: " +
                    tableNameWithDomain + ", Response code: " + collectionAdminResponse.getStatus() +
                    " , errors: " + errors.toString());
        }
        return true;
    }

    public boolean updateSolrSchema(String table, SolrSchema solrSchema, boolean merge)
            throws SolrClientServiceException {
        SolrSchema oldSchema;
        List<SchemaRequest.Update> updateFields = new ArrayList<>();
        SolrClient client = getSolrServiceClientByCollection(table);
        SchemaResponse.UpdateResponse updateResponse;
        try {
            oldSchema = getSolrSchema(table);
        } catch (SolrSchemaNotFoundException e) {
            throw new SolrClientServiceException("Error while retrieving  the Solr schema for table: " + table + ", " +
                    "error: " + e.getMessage(), e);
        }

        updateFields = createUpdateFields(solrSchema, merge, oldSchema, updateFields);
        SchemaRequest.MultiUpdate multiUpdateRequest = new SchemaRequest.MultiUpdate(updateFields);
        try {
            updateResponse = multiUpdateRequest.process(client, table);
            // UpdateResponse does not have a "getErrorMessages()" method, so we check
            // if the errors attribute exists in the response
            Object errors = updateResponse.getResponse().get(ATTR_ERRORS);
            if (updateResponse.getStatus() == 0 && errors == null) {
                return true;
            } else {
                throw new SolrClientServiceException("Couldn't update index schema, Response code: " +
                        updateResponse.getStatus() + ", errors: " + errors);
            }
        } catch (SolrServerException | IOException | SolrException e) {
            throw new SolrClientServiceException("error while updating the index schema for table: " +
                    table + " error: " + e.getMessage(), e);
        }
    }

    private List<SchemaRequest.Update> createUpdateFields(SolrSchema solrSchema, boolean merge,
                                                          SolrSchema finalOldSchema,
                                                          List<SchemaRequest.Update> updateFields) {
        if (!merge) {
            List<SchemaRequest.Update> oldFields = createSolrDeleteFields(finalOldSchema);
            List<SchemaRequest.Update> newFields = createSolrAddFields(solrSchema);
            updateFields.addAll(oldFields);
            updateFields.addAll(newFields);
        } else {
            updateFields = solrSchema.getFields().entrySet().stream()
                    .map(field -> finalOldSchema.getField(field.getKey()) != null ?
                                  updateSchemaAndGetReplaceFields(finalOldSchema, field) :
                                  updateSchemaAndGetAddFields(finalOldSchema, field)).collect(Collectors.toList());
        }
        return updateFields;
    }

    private SchemaRequest.Update updateSchemaAndGetReplaceFields(SolrSchema oldSchema,
                                                                 Map.Entry<String, SolrSchemaField> field) {
        oldSchema.addField(field.getKey(), new SolrSchemaField(field.getValue()));
        return new SchemaRequest.ReplaceField(getSolrIndexProperties(field));
    }

    private SchemaRequest.Update updateSchemaAndGetAddFields(SolrSchema oldSchema,
                                                             Map.Entry<String, SolrSchemaField> field) {
        oldSchema.addField(field.getKey(), new SolrSchemaField(field.getValue()));
        return new SchemaRequest.AddField(getSolrIndexProperties(field));
    }

    private List<SchemaRequest.Update> createSolrAddFields(SolrSchema solrSchema) {
        List<SchemaRequest.Update> fields = new ArrayList<>();
        solrSchema.getFields().entrySet().stream().forEach(field -> {
            Map<String, Object> properties = getSolrIndexProperties(field);
            SchemaRequest.AddField addFieldRequest = new SchemaRequest.AddField(properties);
            fields.add(addFieldRequest);
        });
        return fields;
    }

    private Map<String, Object> getSolrIndexProperties(Map.Entry<String, SolrSchemaField> field) {
        Map<String, Object> properties = new HashMap<>();
        properties.putAll(field.getValue().getProperties());
        return properties;
    }

    private List<SchemaRequest.Update> createSolrDeleteFields(SolrSchema oldSchema) {
        List<SchemaRequest.Update> fields = new ArrayList<>();
        oldSchema.getFields().entrySet().stream().filter(field -> !(field.getKey().equals(oldSchema.getUniqueKey()) ||
                field.getKey().equals(SolrSchemaField.FIELD_VERSION))).forEach(field -> {
            SchemaRequest.DeleteField deleteFieldRequest = new SchemaRequest.DeleteField(field.getKey());
            fields.add(deleteFieldRequest);
        });
        return fields;
    }

    public SolrSchema getSolrSchema(String table)
            throws SolrClientServiceException, SolrSchemaNotFoundException {
        SolrClient client = getSolrServiceClientByCollection(table);
            try {
                if (collectionConfigExists(table)) {
                    SchemaRequest.Fields fieldsRequest = new SchemaRequest.Fields();
                    SchemaRequest.UniqueKey uniqueKeyRequest = new SchemaRequest.UniqueKey();
                    SchemaResponse.FieldsResponse fieldsResponse = fieldsRequest.process(client, table);
                    SchemaResponse.UniqueKeyResponse uniqueKeyResponse = uniqueKeyRequest.process(client, table);
                    List<Map<String, Object>> fields = fieldsResponse.getFields();
                    String uniqueKey = uniqueKeyResponse.getUniqueKey();
                    SolrSchema schema = createSolrSchema(uniqueKey, fields);
                    return schema;
                } else {
                    throw new SolrSchemaNotFoundException("Index schema for table: " + table + "is not found");
                }
            } catch (SolrServerException | IOException | SolrException e) {
                throw new SolrClientServiceException("error while retrieving the index schema for table: " + table +
                        ", error: " + e.getMessage(), e);
            }
    }

    private static SolrSchema createSolrSchema(String uniqueKey, List<Map<String, Object>> fields)
            throws SolrClientServiceException {
        SolrSchema solrSchema = new SolrSchema();
        solrSchema.setUniqueKey(uniqueKey);
        solrSchema.setFields(createIndexFields(fields));
        return solrSchema;
    }

    private static Map<String, SolrSchemaField> createIndexFields(List<Map<String, Object>> fields)
            throws SolrClientServiceException {
        Map<String, SolrSchemaField> indexFields = new LinkedHashMap<>();
        String fieldName;
        for (Map<String, Object> fieldProperties : fields) {
            if (fieldProperties != null && fieldProperties.containsKey(SolrSchemaField.ATTR_FIELD_NAME)) {
                fieldName = fieldProperties.get(SolrSchemaField.ATTR_FIELD_NAME).toString();
                indexFields.put(fieldName, new SolrSchemaField(fieldProperties));
            } else {
                throw new SolrClientServiceException("Fields must have an attribute called " +
                                                     SolrSchemaField.ATTR_FIELD_NAME);
            }
        }
        return indexFields;
    }

    public boolean deleteCollection(String table) throws SolrClientServiceException {
        try {
            if (collectionExists(table)) {
                SiddhiSolrClient client = getSolrServiceClientByCollection(table);
                CollectionConfiguration configuration = tableToConfigMapping.get(table);
                if (configuration == null) {
                    throw new SolrClientServiceException("Configuration is not set for table: " +
                            table + ", call initCollection() first");
                }
                String tableNameWithDomain = SolrTableUtils.getCollectionNameWithDomainName(configuration.getDomainName
                        (), table);
                CollectionAdminRequest.Delete deleteRequest =
                        CollectionAdminRequest.deleteCollection(tableNameWithDomain);
                CollectionAdminResponse deleteRequestResponse =
                        deleteRequest.process(client, tableNameWithDomain);
                if (deleteRequestResponse.isSuccess() && collectionConfigExists(table)) {
                    ConfigSetAdminRequest.Delete configSetAdminRequest = new ConfigSetAdminRequest.Delete();
                    configSetAdminRequest.setConfigSetName(tableNameWithDomain);
                    ConfigSetAdminResponse configSetResponse = configSetAdminRequest.process(client);
                    Object errors = configSetResponse.getErrorMessages();
                    if (configSetResponse.getStatus() == 0 && errors == null) {
                        tableToConfigMapping.remove(table);
                        return tryToCloseClient(configuration);
                    } else {
                        throw new SolrClientServiceException("Error in deleting index for table: " + table + ", " +
                                ", Response code: " + configSetResponse.getStatus() + " , errors: " +
                                errors.toString());
                    }
                }
            }
        } catch (IOException | SolrServerException | SolrException e) {
            log.error("error while deleting the index for table: " + table + ": " + e.getMessage(), e);
            throw new SolrClientServiceException("error while deleting the index for table: " + table + ", error: " +
                                                 e.getMessage(), e);
        }
        return false;
    }

    public boolean tryToCloseClient(CollectionConfiguration configuration) throws IOException {
        synchronized (this) {
            Integer count = urlToTableCountMapping.get(configuration.getSolrServerUrl());
            if (count != null && count > 0) {
                count -= 1;
                urlToTableCountMapping.put(configuration.getSolrServerUrl(), count);
                if (count == 0) {
                    SolrClient client = urlToSolrClientMapping.get(configuration.getSolrServerUrl());
                    client.close();
                    urlToTableCountMapping.remove(configuration.getSolrServerUrl());
                    urlToSolrClientMapping.remove(configuration.getSolrServerUrl());
                }
            } else {
                return false;
            }
        }
        return true;
    }

    public boolean collectionExists(String table) throws SolrClientServiceException {
        CollectionAdminRequest.List listRequest = CollectionAdminRequest.listCollections();
        CollectionConfiguration configuration = tableToConfigMapping.get(table);
        if (configuration == null) {
            throw new SolrClientServiceException("Configuration is not set for table: " +
                    table + ", call initCollection() first");
        }
        String tableWithDomain = SolrTableUtils.getCollectionNameWithDomainName(configuration.getDomainName(), table);
        try {
            CollectionAdminResponse listResponse = listRequest.process(getSolrServiceClientByCollection(table));
            Object errors = listResponse.getErrorMessages();
            if (listResponse.getStatus() == 0 && errors == null) {
                List collections = (List) listResponse.getResponse().get(ATTR_COLLECTIONS);
                return collections.contains(tableWithDomain);
            } else {
                throw new SolrClientServiceException("Error in checking index for table: " + table + ", " +
                        ", Response code: " + listResponse.getStatus() + " , errors: " + errors.toString());
            }
        } catch (IOException | SolrServerException | SolrException e) {
            throw new SolrClientServiceException("Error while checking the existence of index for table : " + table +
                    ", error: " + e.getMessage(), e);
        }
    }

    public boolean collectionConfigExists(String table) throws SolrClientServiceException {
        ConfigSetAdminResponse.List listRequestReponse;
        SiddhiSolrClient siddhiSolrClient = getSolrServiceClientByCollection(table);
        String tableNameWithDomain = SolrTableUtils.getCollectionNameWithDomainName(tableToConfigMapping.get
                (table).getDomainName(), table);
        ConfigSetAdminRequest.List listRequest = new ConfigSetAdminRequest.List();
        try {
            listRequestReponse = listRequest.process(siddhiSolrClient);
            Object errors = listRequestReponse.getErrorMessages();
            if (listRequestReponse.getStatus() == 0 && errors == null) {
                return listRequestReponse.getConfigSets().contains(tableNameWithDomain);
            } else {
                throw new SolrClientServiceException("Error in checking the existance of index configuration for " +
                        "table: '" + table + "', Response code: " +
                        listRequestReponse.getStatus() + " , errors: " + errors.toString());
            }
        } catch (IOException | SolrServerException | SolrException e) {
            throw new SolrClientServiceException("Error while checking if index configurations exists for table: " +
                    table + ", error: " + e.getMessage(), e);
        }
    }

    public void insertDocuments(String table, List<SiddhiSolrDocument> docs, boolean commitAsync)
            throws SolrClientServiceException {
        try {
            SiddhiSolrClient client = getSolrServiceClientByCollection(table);
            client.add(table, SolrTableUtils.getSolrInputDocuments(docs));
            if (!commitAsync) {
                client.commit(table);
            }
        } catch (SolrServerException | IOException e) {
            throw new SolrClientServiceException("Error while inserting the documents to index for table: " +
                    table + ", error: " + e.getMessage(), e);
        }
    }

    public void deleteDocuments(String table, List<String> ids, boolean commitAsync) throws SolrClientServiceException {
        if (ids != null && !ids.isEmpty()) {
            SiddhiSolrClient client = getSolrServiceClientByCollection(table);
            try {
                client.deleteById(table, ids);
                if (!commitAsync) {
                    client.commit(table);
                }
            } catch (SolrServerException | IOException | SolrException e) {
                throw new SolrClientServiceException("Error while deleting index documents by ids, from table: " +
                        table + ", error: " + e.getMessage(), e);
            }
        }
    }

    public void deleteDocuments(String table, String query, boolean commitAsync) throws SolrClientServiceException {
        if (query != null && !query.isEmpty()) {
            SiddhiSolrClient client = getSolrServiceClientByCollection(table);
            try {
                client.deleteByQuery(table, query);
                if (!commitAsync) {
                    client.commit(table);
                }
            } catch (SolrServerException | IOException | SolrException e) {
                throw new SolrClientServiceException("Error while deleting index documents by query, " +
                        e.getMessage(), e);
            }
        }
    }

    public void destroy() throws SolrClientServiceException {
        try {
            if (indexerClient != null) {
                indexerClient.close();
            }
        } catch (IOException | SolrException e) {
            throw new SolrClientServiceException("Error while destroying the indexer service, " + e.getMessage(), e);
        }
        indexerClient = null;
    }
}
