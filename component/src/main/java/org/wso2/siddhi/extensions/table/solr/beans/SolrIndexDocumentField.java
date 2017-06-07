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

package org.wso2.siddhi.extensions.table.solr.beans;

import org.apache.solr.common.SolrInputField;

/**
 * This represents the IndexField which is input to solr index
 */
public class SolrIndexDocumentField extends SolrInputField {

    private static final long serialVersionUID = 7166526003567985768L;

    public SolrIndexDocumentField(String n) {
        super(n);
    }
}
