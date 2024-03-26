package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.InitializeShareGroupStateResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

public class InitializeShareGroupStateResponse extends AbstractResponse {
  private final InitializeShareGroupStateResponseData data;

  public InitializeShareGroupStateResponse(InitializeShareGroupStateResponseData data) {
    super(ApiKeys.INITIALIZE_SHARE_GROUP_STATE);
    this.data = data;
  }

  @Override
  public InitializeShareGroupStateResponseData data() {
    return data;
  }

  @Override
  public Map<Errors, Integer> errorCounts() {
    return Collections.singletonMap(Errors.forCode(data.errorCode()), 1);
  }

  @Override
  public int throttleTimeMs() {
    return DEFAULT_THROTTLE_TIME;
  }

  @Override
  public void maybeSetThrottleTimeMs(int throttleTimeMs) {
    // No op
  }

  public static InitializeShareGroupStateResponse parse(ByteBuffer buffer, short version) {
    return new InitializeShareGroupStateResponse(
        new InitializeShareGroupStateResponseData(new ByteBufferAccessor(buffer), version)
    );
  }
}
