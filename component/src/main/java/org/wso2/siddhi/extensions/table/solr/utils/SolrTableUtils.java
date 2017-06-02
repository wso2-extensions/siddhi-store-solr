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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.wso2.siddhi.extensions.table.solr.SolrCompiledCondition;
import org.wso2.siddhi.extensions.table.solr.beans.SolrIndexDocument;
import org.wso2.siddhi.extensions.table.solr.beans.SolrIndexDocumentField;
import org.wso2.siddhi.extensions.table.solr.beans.SolrSchema;
import org.wso2.siddhi.extensions.table.solr.beans.SolrSchemaField;
import org.wso2.siddhi.extensions.table.solr.exceptions.SolrClientServiceException;
import org.wso2.siddhi.extensions.table.solr.exceptions.SolrTableException;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Paths;
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

    public static final String CUSTOM_WSO2_CONF_DIR_NAME = "conf";
    public static final String WSO2_ANALYTICS_INDEX_CONF_DIRECTORY_SYS_PROP = "wso2_custom_index_conf_dir";
    private static final String tenantDomain = "DEFAULT";
    private static ThreadLocal<SecureRandom> secureRandom = new ThreadLocal<SecureRandom>() {
        protected SecureRandom initialValue() {
            return new SecureRandom();
        }
    };

    public static String getIndexerConfDirectory() throws SolrClientServiceException {
        File confDir = null;
        try {
            confDir = new File(getConfDirectoryPath());
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("Error in getting the indexer config path: " + e.getMessage(), e);
            }
        }
        if (confDir == null || !confDir.exists()) {
            return getCustomIndexerConfDirectory();
        } else {
            return confDir.getAbsolutePath();
        }
    }

    public static String getConfDirectoryPath() {
        String carbonConfigDirPath = System.getProperty("carbon.config.dir.path");
        if (carbonConfigDirPath == null) {
            carbonConfigDirPath = System.getenv("CARBON_CONFIG_DIR_PATH");
            if (carbonConfigDirPath == null) {
                return getBaseDirectoryPath() + File.separator + "conf";
            }
        }
        return carbonConfigDirPath;
    }

    private static String getCustomIndexerConfDirectory() throws SolrClientServiceException {
        String path = System.getProperty(WSO2_ANALYTICS_INDEX_CONF_DIRECTORY_SYS_PROP);
        if (path == null) {
            path = Paths.get("").toAbsolutePath().toString() + File.separator + CUSTOM_WSO2_CONF_DIR_NAME;
        }
        File confDir = new File(path);
        if (!confDir.exists()) {
            throw new SolrClientServiceException("The custom WSO2 index configuration directory does not exist at '" + path + "'. "
                    + "This can be given by correctly pointing to a valid configuration directory by setting the "
                    + "Java system property '" + WSO2_ANALYTICS_INDEX_CONF_DIRECTORY_SYS_PROP + "'.");
        }
        return confDir.getAbsolutePath();
    }

    public static String getBaseDirectoryPath() {
        String baseDir = System.getProperty("analytics.home");
        if (baseDir == null) {
            baseDir = System.getenv("ANALYTICS_HOME");
            System.setProperty("analytics.home", baseDir);
        }
        return baseDir;
    }

    public static File getFileFromSystemResources(String fileName) throws URISyntaxException {
        File file = null;
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        if (classLoader != null) {
            URL url = classLoader.getResource(fileName);
            if (url == null) {
                url = classLoader.getResource(File.separator + fileName);
            }
            file = new File(url.toURI());
        }
        return file;
    }

    public static SolrSchema getMergedIndexSchema(SolrSchema oldSchema, SolrSchema newSchema) {
        SolrSchema mergedSchema = new SolrSchema();
        mergedSchema.setUniqueKey(newSchema.getUniqueKey());
        mergedSchema.setFields(oldSchema.getFields());
        for (Map.Entry<String, SolrSchemaField> indexFieldEntry : newSchema.getFields().entrySet()) {
            mergedSchema.addField(indexFieldEntry.getKey(), indexFieldEntry.getValue());
        }
        return mergedSchema;
    }

    public static Map<String, SolrInputField> getSolrFields(Map<String, SolrIndexDocumentField> fields) {
        Map<String, SolrInputField> solrFields = new LinkedHashMap<>(fields.size());
        solrFields.putAll(fields);
        return solrFields;
    }

    public static List<SolrInputDocument> getSolrInputDocuments(List<SolrIndexDocument> docs) {
        List<SolrInputDocument> solrDocs = new ArrayList<>(docs.size());
        solrDocs.addAll(docs);
        return solrDocs;
    }

    public static String getCollectionNameWithDomainName(String tableName) {
        if (tableName != null) {
            return tenantDomain + "_" + tableName;
        } else {
            return null;
        }
    }

    public static SolrSchema createIndexSchema(String schema) {
        Map<String, SolrSchemaField> schemaFields = new HashMap<>();
        String[] fieldsWithProperties = schema.split(",");
        for (String fieldWithProperties : fieldsWithProperties) {
            Map<String, Object> fieldProperties = new HashMap<>();
            String[] properties = fieldWithProperties.trim().split(" ");
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
            schemaFields.put((String)fieldProperties.get(SolrSchemaField.ATTR_FIELD_NAME), new SolrSchemaField
                    (fieldProperties));
        }
        return new SolrSchema(SolrSchemaField.FIELD_ID, schemaFields);
    }

    public static List<SolrIndexDocument> createSolrDocuments(List<Attribute> attributes, List<String> primaryKeys,
                                                              List<Object[]> records) {
        List<SolrIndexDocument> solrIndexDocuments = new ArrayList<>();
        for (Object[] record : records) {
            SolrIndexDocument solrIndexDocument = createSolrDocument(attributes, primaryKeys, record);
            solrIndexDocuments.add(solrIndexDocument);
        }
        return solrIndexDocuments;
    }

    public static SolrIndexDocument createSolrDocument(List<Attribute> attributes, List<String> primaryKeys,
                                                       Object[] record) {
        int fieldIndex = 0;
        SolrIndexDocument solrIndexDocument = new SolrIndexDocument();
        for (Attribute attribute : attributes) {
            solrIndexDocument.setField(attribute.getName(), record[fieldIndex]);
            fieldIndex++;
        }
        if (!solrIndexDocument.containsKey(SolrSchemaField.FIELD_ID)) {
            String id;
            if (primaryKeys != null && !primaryKeys.isEmpty()) {
                id = generateRecordIdFromPrimaryKeyValues(solrIndexDocument, primaryKeys);
            } else {
                id = generateRecordID();
            }
            solrIndexDocument.addField(SolrSchemaField.FIELD_ID, id);
        }
        return solrIndexDocument;
    }

    public static String resolveCondition(SolrCompiledCondition compiledCondition, Map<String, Object> parameters) {
        String condition = compiledCondition.getCompiledQuery();
        for (Map.Entry<String, Object> entry : parameters.entrySet()) {
            String name = entry.getKey();
            Object value = entry.getValue();
            String namePlaceholder = Pattern.quote("[" + name + "]");
            condition = condition.replaceAll(namePlaceholder, value.toString());
        }

        //set solr "select all" query if condition is "true"
        if (condition.equalsIgnoreCase("\"" + Boolean.TRUE.toString() + "\"")) {
            condition = "*:*";
        }
        return condition;
    }

    public static String generateRecordIdFromPrimaryKeyValues(SolrIndexDocument document, List<String> primaryKeys) {
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
        byte[] data = builder.toString().getBytes(Charset.defaultCharset()); //TODO: UTF8
        return UUID.nameUUIDFromBytes(data).toString();
    }

    public static String generateRecordID() {
        byte[] data = new byte[16];
        secureRandom.get().nextBytes(data);
        ByteBuffer buff = ByteBuffer.wrap(data);
        return new UUID(buff.getLong(), buff.getLong()).toString();
    }

    private static Map<String, Object> extractValues(SolrInputDocument doc) {
        Map<String, Object> values = new HashMap<>();
        for (Map.Entry<String, SolrInputField> entry : doc.entrySet()) {
            values.put(entry.getKey(), entry.getValue().getValue());
        }
        return values;
    }
}
