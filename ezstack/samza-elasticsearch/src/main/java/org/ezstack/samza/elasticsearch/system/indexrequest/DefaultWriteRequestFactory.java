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

    return writeRequest;
  }

  private DocWriteRequest getRequest(OutgoingMessageEnvelope envelope) {
    String[] parts = envelope.getSystemStream().getStream().split("/");

    if (parts.length < 2 || parts.length > 5) {
      throw new SamzaException("Elasticsearch stream name must match pattern {index}/{type} or {index}/{type}/{opCode}/{versionType}/{version}");
    }

    String index = parts[0];
    String type = parts[1];

    if (parts.length == 2) {
      return Requests.indexRequest(index)
              .type(type)
              .id(envelope.getKey().toString())
              .source(getSource(envelope));
    }

    DocWriteRequest request;

    switch (parts[2].toLowerCase()) {
      case "delete":
        request = Requests.deleteRequest(index)
                .type(type)
                .id(envelope.getKey().toString());
        break;
      case "index":
        request = Requests.indexRequest(index)
                .type(type)
                .id(envelope.getKey().toString())
                .source(getSource(envelope));
        break;
      default:
        throw new SamzaException("Invalid Op Code, must be 'delete' or 'index'");
    }

    if (parts.length == 3) {
      return request;
    }

    switch (parts[3].toLowerCase()) {
      case "external":
        request.versionType(VersionType.EXTERNAL);
        break;
      case "external_gte":
        request.versionType(VersionType.EXTERNAL_GTE);
        break;
      case "internal":
        request.versionType(VersionType.INTERNAL);
        break;
      default:
        throw new SamzaException("Invalid version type");

    }

    if (parts.length == 4) {
      if (request.versionType() == VersionType.INTERNAL) {
        return request;
      }
      throw new SamzaException("A version must be provided when version type is external or external_gte");
    }

    request.version(Long.parseLong(parts[4]));

    return request;


  }

  private Optional<String> getRoutingKey(OutgoingMessageEnvelope envelope) {
    Object partitionKey = envelope.getPartitionKey();
    if (partitionKey == null) {
      return Optional.absent();
    }
    return Optional.of(partitionKey.toString());
  }

  private Map getSource(OutgoingMessageEnvelope envelope) {
    Object message = envelope.getMessage();
    if (message instanceof Map) {
      return (Map) message;
    } else {
      throw new SamzaException("Unsupported message type: " + message.getClass().getCanonicalName());
    }
  }

}