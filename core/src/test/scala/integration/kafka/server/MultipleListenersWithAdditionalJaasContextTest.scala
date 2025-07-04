/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.server

import kafka.security.JaasTestUtils
import kafka.security.JaasTestUtils.JaasSection

import java.util.Properties
import scala.collection.Seq
import scala.jdk.OptionConverters._
import scala.jdk.CollectionConverters._


class MultipleListenersWithAdditionalJaasContextTest extends MultipleListenersWithSameSecurityProtocolBaseTest {

  import MultipleListenersWithSameSecurityProtocolBaseTest._

  override def staticJaasSections: Seq[JaasSection] = {
    val (serverKeytabFile, _) = maybeCreateEmptyKeytabFiles()
    Seq(JaasTestUtils.kafkaServerSection("secure_external.KafkaServer", kafkaServerSaslMechanisms(SecureExternal).asJava, Some(serverKeytabFile).toJava))
  }

  override protected def dynamicJaasSections: Properties = {
    val props = new Properties
    kafkaServerSaslMechanisms(SecureInternal).foreach { mechanism =>
      addDynamicJaasSection(props, SecureInternal, mechanism,
        JaasTestUtils.kafkaServerSection("secure_internal.KafkaServer", java.util.List.of(mechanism), None.toJava))
    }
    props
  }
}
