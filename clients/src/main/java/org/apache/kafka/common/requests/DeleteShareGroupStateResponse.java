package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.DeleteShareGroupStateResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

public class DeleteShareGroupStateResponse extends AbstractResponse {
  private final DeleteShareGroupStateResponseData data;

  public DeleteShareGroupStateResponse(DeleteShareGroupStateResponseData data) {
    super(ApiKeys.READ_SHARE_GROUP_STATE);
    this.data = data;
  }

  @Override
  public DeleteShareGroupStateResponseData data() {
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

  public static DeleteShareGroupStateResponse parse(ByteBuffer buffer, short version) {
    return new DeleteShareGroupStateResponse(
        new DeleteShareGroupStateResponseData(new ByteBufferAccessor(buffer), version)
    );
  }
}
