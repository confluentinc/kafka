package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.ReadShareGroupOffsetsStateResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

public class ReadShareGroupOffsetsStateResponse extends AbstractResponse {
  private final ReadShareGroupOffsetsStateResponseData data;

  public ReadShareGroupOffsetsStateResponse(ReadShareGroupOffsetsStateResponseData data) {
    super(ApiKeys.READ_SHARE_GROUP_OFFSETS_STATE);
    this.data = data;
  }

  @Override
  public ReadShareGroupOffsetsStateResponseData data() {
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
  public static ReadShareGroupOffsetsStateResponse parse(ByteBuffer buffer, short version) {
    return new ReadShareGroupOffsetsStateResponse(
        new ReadShareGroupOffsetsStateResponseData(new ByteBufferAccessor(buffer), version)
    );
  }
}
