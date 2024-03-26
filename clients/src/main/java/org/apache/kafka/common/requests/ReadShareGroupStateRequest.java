package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.ReadShareGroupStateRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class ReadShareGroupStateRequest extends AbstractRequest {
  public static class Builder extends AbstractRequest.Builder<ReadShareGroupStateRequest> {

    private final ReadShareGroupStateRequestData data;

    public Builder(ReadShareGroupStateRequestData data) {
      this(data, false);
    }

    public Builder(ReadShareGroupStateRequestData data, boolean enableUnstableLastVersion) {
      super(ApiKeys.READ_SHARE_GROUP_STATE, enableUnstableLastVersion);
      this.data = data;
    }

    @Override
    public ReadShareGroupStateRequest build(short version) {
      return new ReadShareGroupStateRequest(data, version);
    }

    @Override
    public String toString() {
      return data.toString();
    }
  }

  private final ReadShareGroupStateRequestData data;

  public ReadShareGroupStateRequest(ReadShareGroupStateRequestData data, short version) {
    super(ApiKeys.READ_SHARE_GROUP_STATE, version);
    this.data = data;
  }

  @Override
  public ReadShareGroupStateResponse getErrorResponse(int throttleTimeMs, Throwable e) {
    return new ReadShareGroupStateResponse(
        new ReadShareGroupStateResponseData()
            .setErrorCode(Errors.forException(e).code())
    );
  }

  @Override
  public ReadShareGroupStateRequestData data() {
    return data;
  }

  public static ReadShareGroupStateRequest parse(ByteBuffer buffer, short version) {
    return new ReadShareGroupStateRequest(
        new ReadShareGroupStateRequestData(new ByteBufferAccessor(buffer), version),
        version
    );
  }
}
