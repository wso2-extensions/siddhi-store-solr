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

package org.wso2.extension.siddhi.store.solr.utils;

import io.siddhi.query.api.definition.Attribute;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.wso2.extension.siddhi.store.solr.SolrCompiledCondition;
import org.wso2.extension.siddhi.store.solr.beans.SiddhiSolrDocument;
import org.wso2.extension.siddhi.store.solr.beans.SiddhiSolrDocumentField;
import org.wso2.extension.siddhi.store.solr.beans.SolrSchema;
import org.wso2.extension.siddhi.store.solr.beans.SolrSchemaField;
import org.wso2.extension.siddhi.store.solr.exceptions.SolrTableException;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * This class contains the utility methods required by the indexer service.
 */
public class SolrTableUtils {

    private static Log log = LogFactory.getLog(SolrTableUtils.class);
    private static ThreadLocal<SecureRandom> secureRandom = new ThreadLocal<SecureRandom>() {
        protected SecureRandom initialValue() {
            return new SecureRandom();
        }
    };

    public static SolrSchema getMergedIndexSchema(SolrSchema oldSchema, SolrSchema newSchema) {
        SolrSchema mergedSchema = new SolrSchema();
        mergedSchema.setUniqueKey(newSchema.getUniqueKey());
        mergedSchema.setFields(oldSchema.getFields());
        for (Map.Entry<String, SolrSchemaField> indexFieldEntry : newSchema.getFields().entrySet()) {
            mergedSchema.addField(indexFieldEntry.getKey(), indexFieldEntry.getValue());
        }
        return mergedSchema;
    }

    public static Map<String, SolrInputField> getSolrFields(Map<String, SiddhiSolrDocumentField> fields) {
        Map<String, SolrInputField> solrFields = new LinkedHashMap<>(fields.size());
        solrFields.putAll(fields);
        return solrFields;
    }

    public static List<SolrInputDocument> getSolrInputDocuments(List<SiddhiSolrDocument> docs) {
        List<SolrInputDocument> solrDocs = new ArrayList<>(docs.size());
        solrDocs.addAll(docs);
        return solrDocs;
    }

    public static String getCollectionNameWithDomainName(String domain, String tableName) {
        if (tableName != null && domain != null) {
            return domain + "_" + tableName;
        } else {
            return null;
        }
    }

    public static SolrSchema createIndexSchema(String schema) {
        Map<String, SolrSchemaField> schemaFields = new HashMap<>();
        String[] fieldsWithProperties = schema.split("\\s*,\\s*");
        for (String fieldWithProperties : fieldsWithProperties) {
            Map<String, Object> fieldProperties = new HashMap<>();
            String[] properties = fieldWithProperties.trim().split("\\s+");
            if (properties.length > 1) {
                fieldProperties.put(SolrSchemaField.ATTR_FIELD_NAME, properties[0]);
                fieldProperties.put(SolrSchemaField.ATTR_TYPE, properties[1]);
                if (properties.length > 2) {
                    for (int i = 2; i < properties.length; i++) {
                        fieldProperties.put(properties[i], true);
                    }
                }
            } else {
                throw new SolrTableException("At least, the solr schema should contain the name and the type");
            }
            schemaFields.put((String) fieldProperties.get(SolrSchemaField.ATTR_FIELD_NAME), new SolrSchemaField
                    (fieldProperties));
        }
        return new SolrSchema(SolrSchemaField.FIELD_ID, schemaFields);
    }

    public static List<SiddhiSolrDocument> createSolrDocuments(List<Attribute> attributes, List<String> primaryKeys,
                                                              List<Object[]> records) {
        List<SiddhiSolrDocument> siddhiSolrDocuments = new ArrayList<>();
        for (Object[] record : records) {
            SiddhiSolrDocument siddhiSolrDocument = createSolrDocument(attributes, primaryKeys, record);
            siddhiSolrDocuments.add(siddhiSolrDocument);
        }
        return siddhiSolrDocuments;
    }

    public static SiddhiSolrDocument createSolrDocument(List<Attribute> attributes, List<String> primaryKeys,
                                                       Object[] record) {
        int fieldIndex = 0;
        SiddhiSolrDocument siddhiSolrDocument = new SiddhiSolrDocument();
        for (Attribute attribute : attributes) {
            siddhiSolrDocument.setField(attribute.getName(), record[fieldIndex]);
            fieldIndex++;
        }
        if (!siddhiSolrDocument.containsKey(SolrSchemaField.FIELD_ID)) {
            String id;
            if (primaryKeys != null && !primaryKeys.isEmpty()) {
                id = generateRecordIdFromPrimaryKeyValues(siddhiSolrDocument, primaryKeys);
            } else {
                id = generateRecordID();
            }
            siddhiSolrDocument.addField(SolrSchemaField.FIELD_ID, id);
        }
        return siddhiSolrDocument;
    }

    public static String resolveCondition(SolrCompiledCondition compiledCondition, Map<String, Object> parameters,
                                          String collection) {
        String condition = compiledCondition.getCompiledQuery();
        if (log.isDebugEnabled()) {
            log.debug("compiled condition for collection '" + collection + "': " + condition);
        }
        for (Map.Entry<String, Object> entry : parameters.entrySet()) {
            String name = entry.getKey();
            Object value = entry.getValue();
            String namePlaceholder = Pattern.quote("[" + name + "]");
            condition = condition.replaceAll(namePlaceholder, value.toString());
        }
        //set solr "select all" query if condition is not provided
        if (condition == null || condition.isEmpty()) {
            condition = "*:*";
        }
        if (log.isDebugEnabled()) {
            log.debug("Resolved condition for collection '" + collection + "': " + condition);
        }
        return condition;
    }

    public static String generateRecordIdFromPrimaryKeyValues(SiddhiSolrDocument document, List<String> primaryKeys) {
        StringBuilder builder = new StringBuilder();
        Object obj;
        for (String key : primaryKeys) {
            obj = document.getFieldValue(key);
            if (obj != null) {
                builder.append(obj.toString());
            }
        }
        /* to make sure, we don't have an empty string */
        builder.append("");
        byte[] data = builder.toString().getBytes(Charset.forName("UTF-8"));
        return UUID.nameUUIDFromBytes(data).toString();
    }

    public static String generateRecordID() {
        byte[] data = new byte[16];
        secureRandom.get().nextBytes(data);
        ByteBuffer buff = ByteBuffer.wrap(data);
        return new UUID(buff.getLong(), buff.getLong()).toString();
    }

    public static String normalizeURL(String solrServerUrl) {
        String serverUrl = solrServerUrl;
        if (serverUrl.endsWith("/")) {
            serverUrl = serverUrl.substring(0, serverUrl.length() - 1);
        }
        return serverUrl;
    }
}
