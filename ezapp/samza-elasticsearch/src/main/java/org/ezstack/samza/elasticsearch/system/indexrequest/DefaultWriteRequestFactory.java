/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.ezstack.samza.elasticsearch.system.indexrequest;

import org.apache.samza.SamzaException;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.Requests;
import com.google.common.base.Optional;
import org.elasticsearch.index.VersionType;

import java.util.Map;

/**
 * The default {@link WriteRequestFactory}.
 *
 * <p>Samza concepts are mapped to Elastic search concepts as follows:</p>
 *
 * <ul>
 *   <li>
 *     The index and type are derived from the stream name using
 *     the following the pattern "{index-name}/{type-name}".
 *   </li>
 *   <li>
 *     The id of the document is the {@link String} representation of the
 *     {@link OutgoingMessageEnvelope#getKey()} from {@link Object#toString()} if provided.</li>
 *   <li>
 *     The source of the document is set from the {@link OutgoingMessageEnvelope#getMessage()}.
 *     Supported types are {@link byte[]} and {@link Map} which are both
 *     passed on without serialising.
 *   </li>
 *   <li>
 *     The routing key is set from {@link String} representation of the
 *     {@link OutgoingMessageEnvelope#getPartitionKey()} from {@link Object#toString()} if provided.
 *   </li>
 * </ul>
 */
public class DefaultWriteRequestFactory implements WriteRequestFactory {

  @Override
  public DocWriteRequest getWriteRequest(OutgoingMessageEnvelope envelope) {
    DocWriteRequest writeRequest = getRequest(envelope);

    Optional<String> routingKey = getRoutingKey(envelope);
    if (routingKey.isPresent()) {
      writeRequest.routing(routingKey.get());
    }

    Optional<VersionType> versionType = getVersionType(envelope);
    if (versionType.isPresent()) {
      writeRequest.versionType(versionType.get());
    }

    return writeRequest;
  }

  protected DocWriteRequest getRequest(OutgoingMessageEnvelope envelope) {
    String[] parts = envelope.getSystemStream().getStream().split("/");

    if (parts.length < 2 || parts.length > 4) {
      throw new SamzaException("Elasticsearch stream name must match pattern {index}/{type} or {index}/{type}/DELETE/{version}");
    }

    String index = parts[0];
    String type = parts[1];

    if (parts.length == 2) {
      return Requests.indexRequest(index)
              .type(type)
              .id(envelope.getKey().toString())
              .source(getSource(envelope));
    }

    if (!parts[2].equals("DELETE")) {
       throw new SamzaException("DELETE is currently the only supported Op Code");
    }

    DeleteRequest deleteRequest = Requests.deleteRequest(index).type(type).id(envelope.getKey().toString());

    if (parts.length == 4) {
      deleteRequest.version(Integer.parseInt(parts[4]));
    }
    return deleteRequest;
  }

  protected Optional<String> getRoutingKey(OutgoingMessageEnvelope envelope) {
    Object partitionKey = envelope.getPartitionKey();
    if (partitionKey == null) {
      return Optional.absent();
    }
    return Optional.of(partitionKey.toString());
  }


  protected Optional<VersionType> getVersionType(OutgoingMessageEnvelope envelope) {
    return Optional.absent();
  }

  protected Map getSource(OutgoingMessageEnvelope envelope) {
    Object message = envelope.getMessage();
    if (message instanceof Map) {
      return (Map) message;
    } else {
      throw new SamzaException("Unsupported message type: " + message.getClass().getCanonicalName());
    }
  }

}