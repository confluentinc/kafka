package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.WriteShareGroupStateResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

public class WriteShareGroupStateResponse extends AbstractResponse {
  private final WriteShareGroupStateResponseData data;

  public WriteShareGroupStateResponse(WriteShareGroupStateResponseData data) {
    super(ApiKeys.READ_SHARE_GROUP_STATE);
    this.data = data;
  }

  @Override
  public WriteShareGroupStateResponseData data() {
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

  public static WriteShareGroupStateResponse parse(ByteBuffer buffer, short version) {
    return new WriteShareGroupStateResponse(
        new WriteShareGroupStateResponseData(new ByteBufferAccessor(buffer), version)
    );
  }
}
