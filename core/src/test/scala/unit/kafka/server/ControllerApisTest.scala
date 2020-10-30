package unit.kafka.server

import java.net.InetAddress
import java.util.UUID

import kafka.network.RequestChannel
import kafka.server.{ClientQuotaManager, ClientRequestQuotaManager, ControllerApis, ControllerMutationQuotaManager, KafkaConfig, MetaProperties, ReplicationQuotaManager}
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.BrokerRegistrationRequestData
import org.apache.kafka.common.network.{ClientInformation, ListenerName, Send}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, BrokerRegistrationRequest, BrokerRegistrationResponse, ByteBufferChannel, RequestContext, RequestHeader, ResponseHeader}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.controller.Controller
import org.apache.kafka.metadata.VersionRange
import org.apache.kafka.server.authorizer.Authorizer
import org.easymock.{Capture, EasyMock}
import org.junit.Assert.{assertEquals, fail}
import org.junit.{After, Test}

class ControllerApisTest {
  // Mocks
  private val brokerID = 1
  private val brokerRack = "Rack1"
  private val clientID = "Client1"
  private val time = new MockTime
  private val incarnationID = UUID.randomUUID()
  private val clusterID = UUID.randomUUID()
  private val metaProperties = new MetaProperties(incarnationID, clusterID)
  private val requestChannelMetrics: RequestChannel.Metrics = EasyMock.createNiceMock(classOf[RequestChannel.Metrics])
  private val requestChannel: RequestChannel = EasyMock.createNiceMock(classOf[RequestChannel])
  private val clientQuotaManager: ClientQuotaManager = EasyMock.createNiceMock(classOf[ClientQuotaManager])
  private val clientRequestQuotaManager: ClientRequestQuotaManager = EasyMock.createNiceMock(classOf[ClientRequestQuotaManager])
  private val clientControllerQuotaManager: ControllerMutationQuotaManager = EasyMock.createNiceMock(classOf[ControllerMutationQuotaManager])
  private val replicaQuotaManager: ReplicationQuotaManager = EasyMock.createNiceMock(classOf[ReplicationQuotaManager])
  private val quotas = QuotaManagers(
    clientQuotaManager,
    clientQuotaManager,
    clientRequestQuotaManager,
    clientControllerQuotaManager,
    replicaQuotaManager,
    replicaQuotaManager,
    replicaQuotaManager,
    None)
  private val controller: Controller = EasyMock.createNiceMock(classOf[Controller])

  private def createControllerApis(authorizer: Option[Authorizer] = None,
                                   supportedFeatures: Map[String, VersionRange] = Map.empty): ControllerApis = {
    val config = TestUtils.createBrokerConfig(brokerID, TestUtils.MockZkConnect)
    new ControllerApis(
      requestChannel,
      authorizer,
      quotas,
      time,
      supportedFeatures,
      controller,
      KafkaConfig.fromProps(config),
      metaProperties
    )
  }

  /**
   * Build a RequestChannel.Request from the AbstractRequest
   *
   * @param request - AbstractRequest
   * @param listenerName - Default listener for the RequestChannel
   * @tparam T - Type of AbstractRequest
   * @return
   */
  private def buildRequest[T <: AbstractRequest](request: AbstractRequest,
                                                 listenerName: ListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)): RequestChannel.Request = {
    val buffer = request.serialize(new RequestHeader(request.api, request.version, clientID, 0))

    // read the header from the buffer first so that the body can be read next from the Request constructor
    val header = RequestHeader.parse(buffer)
    val context = new RequestContext(header, "1", InetAddress.getLocalHost, KafkaPrincipal.ANONYMOUS,
      listenerName, SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false)
    new RequestChannel.Request(processor = 1, context = context, startTimeNanos = 0, MemoryPool.NONE, buffer,
      requestChannelMetrics)
  }

  /**
   * Exercises the serialize/deserialize logic for API responses
   * Useful for ensuring version checks are working as intended
   *
   * @param api - API type
   * @param request - Request
   * @param capturedResponse - Captured response before send
   * @return
   */
  private def simulateSendResponse(api: ApiKeys, apiVersion: Short, request: RequestChannel.Request, capturedResponse: Option[AbstractResponse]): AbstractResponse = {
    capturedResponse match {
      case Some(response) =>
        val responseSend = request.context.buildResponse(response)
        val channel = new ByteBufferChannel(responseSend.size)
        responseSend.writeTo(channel)
        channel.close()
        channel.buffer.getInt()
        ResponseHeader.parse(channel.buffer, api.responseHeaderVersion(apiVersion))
        AbstractResponse.parseResponse(api, api.responseSchema(apiVersion).read(channel.buffer), apiVersion)
      case None =>
        fail("Empty response not expected")
        null
    }
  }

  @Test
  def testBrokerRegistration(): Unit = {
    val brokerRegistrationRequest = new BrokerRegistrationRequest.Builder(
      new BrokerRegistrationRequestData()
        .setBrokerId(brokerID)
        .setRack(brokerRack)
    ).build()

    val request = buildRequest(brokerRegistrationRequest)

    val capturedResponse: Capture[Option[AbstractResponse]] = EasyMock.newCapture()
    EasyMock.expect(requestChannel.sendResponse(
      EasyMock.anyObject[RequestChannel.Request](),
      EasyMock.capture(capturedResponse),
      EasyMock.anyObject[Option[Send => Unit]]()
    ))


    EasyMock.replay(requestChannel)
    createControllerApis().handleBrokerRegistration(request)

    val simulatedResponse = simulateSendResponse(
      ApiKeys.BROKER_REGISTRATION,
      brokerRegistrationRequest.version,
      request,
      capturedResponse.getValue)

    val response = simulatedResponse.asInstanceOf[BrokerRegistrationResponse]
    assertEquals(response.errorCounts(), 0)
  }

  @After
  def tearDown(): Unit = {
    quotas.shutdown()
  }
}
