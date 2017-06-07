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

package org.wso2.siddhi.extensions.table.solr.impl;

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
import org.wso2.siddhi.extensions.table.solr.beans.SolrIndexDocument;
import org.wso2.siddhi.extensions.table.solr.beans.SolrSchema;
import org.wso2.siddhi.extensions.table.solr.beans.SolrSchemaField;
import org.wso2.siddhi.extensions.table.solr.config.CollectionConfiguration;
import org.wso2.siddhi.extensions.table.solr.exceptions.SolrClientServiceException;
import org.wso2.siddhi.extensions.table.solr.exceptions.SolrSchemaNotFoundException;
import org.wso2.siddhi.extensions.table.solr.utils.SolrTableUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * This class represents a concrete implementation of {@link org.wso2.siddhi.extensions.table.solr.SolrClientService}
 */
public class SolrClientServiceImpl {

    private static final String ATTR_ERRORS = "errors";
    private static final String ATTR_COLLECTIONS = "collections";
    private static final String SOLR_CONFIG_FILE = "solr-cloud-config.xml";
    private static Log log = LogFactory.getLog(SolrClientServiceImpl.class);
    private volatile SiddhiSolrClient indexerClient = null;
    private CollectionConfiguration glabalCollectionConfig;
    private Map<String, SolrSchema> solrSchemaCache = new ConcurrentHashMap<>();
    private static SolrClientServiceImpl solrClientService = new SolrClientServiceImpl();

    private SolrClientServiceImpl() {

    }

    public static SolrClientServiceImpl  getInstance() {
        return solrClientService;
    }

    public SiddhiSolrClient getSolrServiceClient() throws SolrClientServiceException {
        if (indexerClient == null) {
            synchronized (this) {
                if (indexerClient == null) {
                    SolrClient client = new CloudSolrClient.Builder().withZkHost(glabalCollectionConfig.getSolrServerUrl()).build();
                    indexerClient = new SiddhiSolrClient(client);
                }
            }
        }
        return indexerClient;
    }

    public boolean createCollection(CollectionConfiguration config)
            throws SolrClientServiceException {
        String table = config.getCollectionName();
        if (glabalCollectionConfig == null) {
            glabalCollectionConfig = config;
        }
        String tableNameWithTenant = SolrTableUtils.getCollectionNameWithDomainName(table);
        try {
            if (!collectionExists(table)) {
                if (!collectionConfigExists(table)) {
                    ConfigSetAdminResponse configSetResponse = createInitialSolrCollectionConfig(config);
                    Object errors = configSetResponse.getErrorMessages();
                    if (configSetResponse.getStatus() == 0 && errors == null) {
                        return createSolrCollection(tableNameWithTenant, config);
                    } else {
                        throw new SolrClientServiceException("Error in deploying initial solr configset for " +
                                                             "table: " + tableNameWithTenant + ", " +
                                ", Response code: " + configSetResponse.getStatus() + " , errors: " + errors.toString());
                    }
                } else {
                    return createSolrCollection(tableNameWithTenant, config);
                }
            }
            return false;
        } catch (SolrServerException | IOException e) {
            throw new SolrClientServiceException("error while creating the index for table: " + table + ": " + e.getMessage(), e);
        }
    }

    /*
    This method is to create the initial index configurations for the index of a table. This will include a default
    indexSchema and other Solr configurations. Later by using updateSolrSchema we can edit the index schema
    */
    private ConfigSetAdminResponse createInitialSolrCollectionConfig(CollectionConfiguration config)
            throws SolrServerException, IOException,
                   SolrClientServiceException {
        String tableNameWithTenant = SolrTableUtils.getCollectionNameWithDomainName(config.getCollectionName());
        ConfigSetAdminRequest.Create configSetAdminRequest = new ConfigSetAdminRequest.Create();
        if (config.getConfigSet() != null && !config.getConfigSet().trim().isEmpty()) {
            configSetAdminRequest.setBaseConfigSetName(config.getConfigSet());
        } else {
            throw new SolrClientServiceException("Base configset cannot be found");
        }
        configSetAdminRequest.setConfigSetName(tableNameWithTenant);
        return configSetAdminRequest.process(getSolrServiceClient());
    }

    private boolean createSolrCollection(String tableNameWithTenant, CollectionConfiguration config)
            throws SolrServerException, IOException, SolrClientServiceException {
        CollectionAdminRequest.Create createRequest =
                CollectionAdminRequest.createCollection(tableNameWithTenant, tableNameWithTenant,
                                                        config.getNoOfShards(),
                                                        config.getNoOfReplicas());
        createRequest.setMaxShardsPerNode(config.getNoOfShards());
        CollectionAdminResponse collectionAdminResponse = createRequest.process(getSolrServiceClient());
        if (!collectionAdminResponse.isSuccess()) {
            Object errors = collectionAdminResponse.getErrorMessages();
            throw new SolrClientServiceException("Error in deploying initial solr configset for collection: " +
                                                 tableNameWithTenant + ", Response code: " + collectionAdminResponse
                    .getStatus() + " , errors: " + errors.toString());
        }
        return true;
    }

    public boolean updateSolrSchema(String table, SolrSchema solrSchema, boolean merge)
            throws SolrClientServiceException {
        SolrSchema oldSchema;
        List<SchemaRequest.Update> updateFields = new ArrayList<>();
        SolrClient client = getSolrServiceClient();
        String tableNameWithTenantDomain = SolrTableUtils.getCollectionNameWithDomainName(table);
        SchemaResponse.UpdateResponse updateResponse;
        try {
            oldSchema = getSolrSchema(table);
        } catch (SolrSchemaNotFoundException e) {
            throw new SolrClientServiceException("Error while retrieving  the Solr schema for table: " + table, e);
        }
        updateFields = createUpdateFields(solrSchema, merge, oldSchema, updateFields);
        SchemaRequest.MultiUpdate multiUpdateRequest = new SchemaRequest.MultiUpdate(updateFields);
        try {
            updateResponse = multiUpdateRequest.process(client, table);
            // UpdateResponse does not have a "getErrorMessages()" method, so we check if the errors attribute exists
            // in the response
            Object errors = updateResponse.getResponse().get(ATTR_ERRORS);
            if (updateResponse.getStatus() == 0 && errors == null) {
                if (merge) {
                    SolrSchema mergedSchema = SolrTableUtils.getMergedIndexSchema(oldSchema, solrSchema);
                    solrSchemaCache.put(tableNameWithTenantDomain, mergedSchema);
                } else {
                    solrSchemaCache.put(tableNameWithTenantDomain, solrSchema);
                }
                return true;
            } else {
                throw new SolrClientServiceException("Couldn't update index schema, Response code: " + updateResponse.getStatus() +
                        ", Errors: " + errors);
            }
        } catch (SolrServerException | IOException e) {
            throw new SolrClientServiceException("error while updating the index schema for table: " + table + ": " + e.getMessage(), e);
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
                    .map(field -> finalOldSchema.getField(field.getKey()) != null ? updateSchemaAndGetReplaceFields(finalOldSchema, field) :
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
        SolrClient client = getSolrServiceClient();
        String tableNameWithTenantDomain = SolrTableUtils.getCollectionNameWithDomainName(table);
        SolrSchema solrSchema = solrSchemaCache.get(tableNameWithTenantDomain);
        if (solrSchema == null) {
            try {
                if (collectionConfigExists(table)) {
                    SchemaRequest.Fields fieldsRequest = new SchemaRequest.Fields();
                    SchemaRequest.UniqueKey uniqueKeyRequest = new SchemaRequest.UniqueKey();
                    SchemaResponse.FieldsResponse fieldsResponse = fieldsRequest.process(client, table);
                    SchemaResponse.UniqueKeyResponse uniqueKeyResponse = uniqueKeyRequest.process(client, table);
                    List<Map<String, Object>> fields = fieldsResponse.getFields();
                    String uniqueKey = uniqueKeyResponse.getUniqueKey();
                    solrSchema = createSolrSchema(uniqueKey, fields);
                    solrSchemaCache.put(tableNameWithTenantDomain, solrSchema);
                } else {
                    throw new SolrSchemaNotFoundException("Index schema for table: " + table + "is not found");
                }
            } catch (SolrServerException | IOException | SolrException e) {
                throw new SolrClientServiceException("error while retrieving the index schema for table: " + table + ": " + e.getMessage(), e);
            }
        }
        return solrSchema;
    }

    private static SolrSchema createSolrSchema(String uniqueKey, List<Map<String, Object>> fields) throws
                                                                                                   SolrClientServiceException {
        SolrSchema solrSchema = new SolrSchema();
        solrSchema.setUniqueKey(uniqueKey);
        solrSchema.setFields(createIndexFields(fields));
        return solrSchema;
    }

    private static Map<String, SolrSchemaField> createIndexFields(List<Map<String, Object>> fields) throws
                                                                                                    SolrClientServiceException {
        Map<String, SolrSchemaField> indexFields = new LinkedHashMap<>();
        String fieldName;
        for (Map<String, Object> fieldProperties : fields) {
            if (fieldProperties != null && fieldProperties.containsKey(SolrSchemaField.ATTR_FIELD_NAME)) {
                fieldName = fieldProperties.remove(SolrSchemaField.ATTR_FIELD_NAME).toString();
                indexFields.put(fieldName, new SolrSchemaField(fieldProperties));
            } else {
                throw new SolrClientServiceException("Fields must have an attribute called " + SolrSchemaField.ATTR_FIELD_NAME);
            }
        }
        return indexFields;
    }

    public boolean deleteCollection(String table) throws SolrClientServiceException {
        try {
            if (collectionExists(table)) {
                String tableNameWithTenant = SolrTableUtils.getCollectionNameWithDomainName(table);
                CollectionAdminRequest.Delete deleteRequest = CollectionAdminRequest.deleteCollection(tableNameWithTenant);
                CollectionAdminResponse deleteRequestResponse =
                        deleteRequest.process(getSolrServiceClient(), tableNameWithTenant);
                if (deleteRequestResponse.isSuccess() && collectionConfigExists(table)) {
                    ConfigSetAdminRequest.Delete configSetAdminRequest = new ConfigSetAdminRequest.Delete();
                    configSetAdminRequest.setConfigSetName(tableNameWithTenant);
                    ConfigSetAdminResponse configSetResponse = configSetAdminRequest.process(getSolrServiceClient());
                    solrSchemaCache.remove(tableNameWithTenant);
                    Object errors = configSetResponse.getErrorMessages();
                    if (configSetResponse.getStatus() == 0 && errors == null) {
                        return true;
                    } else {
                        throw new SolrClientServiceException("Error in deleting index for table: " + table + ", " +
                                                   ", Response code: " + configSetResponse.getStatus() + " , errors: " + errors.toString());
                    }
                }
            }
        } catch (IOException | SolrServerException e) {
            log.error("error while deleting the index for table: " + table + ": " + e.getMessage(), e);
            throw new SolrClientServiceException("error while deleting the index for table: " + table + ": " + e.getMessage(), e);
        }
        return false;
    }

    public boolean collectionExists(String table) throws SolrClientServiceException {
        CollectionAdminRequest.List listRequest = CollectionAdminRequest.listCollections();
        String tableWithTenant = SolrTableUtils.getCollectionNameWithDomainName(table);
        try {
            CollectionAdminResponse listResponse = listRequest.process(getSolrServiceClient());
            Object errors = listResponse.getErrorMessages();
            if (listResponse.getStatus() == 0 && errors == null) {
                List collections = (List) listResponse.getResponse().get(ATTR_COLLECTIONS);
                return collections.contains(tableWithTenant);
            } else {
                throw new SolrClientServiceException("Error in checking index for table: " + table + ", " +
                        ", Response code: " + listResponse.getStatus() + " , errors: " + errors.toString());
            }
        } catch (IOException | SolrServerException e) {
            throw new SolrClientServiceException("Error while checking the existence of index for table : " + table, e);
        }
    }

    public boolean collectionConfigExists(String table) throws SolrClientServiceException {
        ConfigSetAdminResponse.List listRequestReponse;
        SiddhiSolrClient siddhiSolrClient = getSolrServiceClient();
        String tableNameWithTenantDomain = SolrTableUtils.getCollectionNameWithDomainName(table);
        ConfigSetAdminRequest.List listRequest = new ConfigSetAdminRequest.List();
        try {
            listRequestReponse = listRequest.process(siddhiSolrClient);
            Object errors = listRequestReponse.getErrorMessages();
            if (listRequestReponse.getStatus()== 0 && errors == null) {
                return listRequestReponse.getConfigSets().contains(tableNameWithTenantDomain);
            } else {
                throw new SolrClientServiceException("Error in checking the existance of index configuration for table: " + table + ", " +
                        ", Response code: " + listRequestReponse.getStatus() + " , errors: " + errors.toString());
            }
        } catch (IOException | SolrServerException e) {
            throw new SolrClientServiceException("Error while checking if index configurations exists for table: " + table, e);
        }
    }

    public void insertDocuments(String table, List<SolrIndexDocument> docs, boolean commitAsync) throws SolrClientServiceException {
        try {
            SiddhiSolrClient client = getSolrServiceClient();
            client.add(table, SolrTableUtils.getSolrInputDocuments(docs));
            if (!commitAsync) {
                client.commit(table);
            }
        } catch (SolrServerException | IOException e) {
            throw new SolrClientServiceException("Error while inserting the documents to index for table: " + table, e);
        }
    }

    public void deleteDocuments(String table, List<String> ids, boolean commitAsync) throws SolrClientServiceException {
        if (ids != null && !ids.isEmpty()) {
            SiddhiSolrClient client = getSolrServiceClient();
            try {
                client.deleteById(table, ids);
                if (!commitAsync) {
                    client.commit(table);
                }
            } catch (SolrServerException | IOException e) {
                throw new SolrClientServiceException("Error while deleting index documents by ids, " + e.getMessage(), e);
            }
        }
    }

    public void deleteDocuments(String table, String query, boolean commitAsync) throws SolrClientServiceException {
        if (query != null && !query.isEmpty()) {
            SiddhiSolrClient client = getSolrServiceClient();
            try {
                client.deleteByQuery(table, query);
                if (!commitAsync) {
                    client.commit(table);
                }
            } catch (SolrServerException | IOException e) {
                throw new SolrClientServiceException("Error while deleting index documents by query, " + e.getMessage(), e);
            }
        }
    }

    public void destroy() throws SolrClientServiceException {
        try {
            if (indexerClient != null) {
                indexerClient.close();
            }
        } catch (IOException e) {
            throw new SolrClientServiceException("Error while destroying the indexer service, " + e.getMessage(), e);
        }
        indexerClient = null;
    }
}
