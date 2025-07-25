/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import org.apache.kafka.clients.NodeApiVersions
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion
import org.apache.kafka.common.message.ApiMessageType
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{ApiVersionsRequest, ApiVersionsResponse, RequestUtils}
import org.apache.kafka.common.test.ClusterInstance
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.IntegrationTestUtils
import org.apache.kafka.server.common.{EligibleLeaderReplicasVersion, GroupVersion, MetadataVersion, ShareVersion, StreamsVersion, TransactionVersion}
import org.apache.kafka.test.TestUtils
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Tag

import scala.jdk.CollectionConverters._

@Tag("integration")
abstract class AbstractApiVersionsRequestTest(cluster: ClusterInstance) {

  def sendUnsupportedApiVersionRequest(request: ApiVersionsRequest): ApiVersionsResponse = {
    val overrideHeader = IntegrationTestUtils.nextRequestHeader(ApiKeys.API_VERSIONS, Short.MaxValue)
    val socket = IntegrationTestUtils.connect(cluster.boundPorts().get(0))
    try {
      val serializedBytes = Utils.toArray(
        RequestUtils.serialize(overrideHeader.data, overrideHeader.headerVersion, request.data, request.version))
      IntegrationTestUtils.sendRequest(socket, serializedBytes)
      IntegrationTestUtils.receive[ApiVersionsResponse](socket, ApiKeys.API_VERSIONS, 0.toShort)
    } finally socket.close()
  }

  def validateApiVersionsResponse(
    apiVersionsResponse: ApiVersionsResponse,
    listenerName: ListenerName = cluster.clientListener(),
    enableUnstableLastVersion: Boolean = false,
    clientTelemetryEnabled: Boolean = false,
    apiVersion: Short = ApiKeys.API_VERSIONS.latestVersion
  ): Unit = {
    if (apiVersion >= 3) {
      assertEquals(6, apiVersionsResponse.data().finalizedFeatures().size())
      assertEquals(MetadataVersion.latestTesting().featureLevel(), apiVersionsResponse.data().finalizedFeatures().find(MetadataVersion.FEATURE_NAME).minVersionLevel())
      assertEquals(MetadataVersion.latestTesting().featureLevel(), apiVersionsResponse.data().finalizedFeatures().find(MetadataVersion.FEATURE_NAME).maxVersionLevel())

      assertEquals(7, apiVersionsResponse.data().supportedFeatures().size())
      assertEquals(MetadataVersion.MINIMUM_VERSION.featureLevel(), apiVersionsResponse.data().supportedFeatures().find(MetadataVersion.FEATURE_NAME).minVersion())
      if (apiVersion < 4) {
        assertEquals(1, apiVersionsResponse.data().supportedFeatures().find("kraft.version").minVersion())
      } else {
        assertEquals(0, apiVersionsResponse.data().supportedFeatures().find("kraft.version").minVersion())
      }
      assertEquals(MetadataVersion.latestTesting().featureLevel(), apiVersionsResponse.data().supportedFeatures().find(MetadataVersion.FEATURE_NAME).maxVersion())

      assertEquals(0, apiVersionsResponse.data().supportedFeatures().find(TransactionVersion.FEATURE_NAME).minVersion())
      assertEquals(TransactionVersion.TV_2.featureLevel(), apiVersionsResponse.data().supportedFeatures().find(TransactionVersion.FEATURE_NAME).maxVersion())

      assertEquals(0, apiVersionsResponse.data().supportedFeatures().find(GroupVersion.FEATURE_NAME).minVersion())
      assertEquals(GroupVersion.GV_1.featureLevel(), apiVersionsResponse.data().supportedFeatures().find(GroupVersion.FEATURE_NAME).maxVersion())

      assertEquals(0, apiVersionsResponse.data().supportedFeatures().find(EligibleLeaderReplicasVersion.FEATURE_NAME).minVersion())
      assertEquals(EligibleLeaderReplicasVersion.ELRV_1.featureLevel(), apiVersionsResponse.data().supportedFeatures().find(EligibleLeaderReplicasVersion.FEATURE_NAME).maxVersion())

      assertEquals(0, apiVersionsResponse.data().supportedFeatures().find(ShareVersion.FEATURE_NAME).minVersion())
      assertEquals(ShareVersion.SV_1.featureLevel(), apiVersionsResponse.data().supportedFeatures().find(ShareVersion.FEATURE_NAME).maxVersion())

      assertEquals(0, apiVersionsResponse.data().supportedFeatures().find(StreamsVersion.FEATURE_NAME).minVersion())
      assertEquals(StreamsVersion.SV_1.featureLevel(), apiVersionsResponse.data().supportedFeatures().find(StreamsVersion.FEATURE_NAME).maxVersion())
    }
    val expectedApis = if (cluster.controllerListenerName() == listenerName) {
      ApiVersionsResponse.collectApis(
        ApiMessageType.ListenerType.CONTROLLER,
        ApiKeys.apisForListener(ApiMessageType.ListenerType.CONTROLLER),
        enableUnstableLastVersion
      )
    } else {
      ApiVersionsResponse.intersectForwardableApis(
        ApiMessageType.ListenerType.BROKER,
        NodeApiVersions.create(ApiKeys.controllerApis().asScala.map(ApiVersionsResponse.toApiVersion).asJava).allSupportedApiVersions(),
        enableUnstableLastVersion,
        clientTelemetryEnabled
      )
    }

    assertEquals(expectedApis.size, apiVersionsResponse.data.apiKeys.size,
      "API keys in ApiVersionsResponse must match API keys supported by broker.")

    val defaultApiVersionsResponse = if (cluster.controllerListenerName() == listenerName) {
      TestUtils.defaultApiVersionsResponse(0, ListenerType.CONTROLLER, enableUnstableLastVersion)
    } else {
      TestUtils.createApiVersionsResponse(0, expectedApis)
    }

    for (expectedApiVersion: ApiVersion <- defaultApiVersionsResponse.data.apiKeys.asScala) {
      val actualApiVersion = apiVersionsResponse.apiVersion(expectedApiVersion.apiKey)
      assertNotNull(actualApiVersion, s"API key ${expectedApiVersion.apiKey()} is supported by broker, but not received in ApiVersionsResponse.")
      assertEquals(expectedApiVersion.apiKey, actualApiVersion.apiKey, "API key must be supported by the broker.")
      assertEquals(expectedApiVersion.minVersion, actualApiVersion.minVersion, s"Received unexpected min version for API key ${actualApiVersion.apiKey}.")
      assertEquals(expectedApiVersion.maxVersion, actualApiVersion.maxVersion, s"Received unexpected max version for API key ${actualApiVersion.apiKey}.")
    }

    if (listenerName.equals(cluster.clientListener))
      assertEquals(ApiKeys.PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION, apiVersionsResponse.apiVersion(ApiKeys.PRODUCE.id).minVersion)
  }
}
