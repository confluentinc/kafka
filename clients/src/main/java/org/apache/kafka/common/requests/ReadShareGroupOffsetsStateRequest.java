package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.ReadShareGroupOffsetsStateRequestData;
import org.apache.kafka.common.message.ReadShareGroupOffsetsStateResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class ReadShareGroupOffsetsStateRequest extends AbstractRequest {
  public static class Builder extends AbstractRequest.Builder<ReadShareGroupOffsetsStateRequest> {

    private final ReadShareGroupOffsetsStateRequestData data;

    public Builder(ReadShareGroupOffsetsStateRequestData data) {
      this(data, false);
    }

    public Builder(ReadShareGroupOffsetsStateRequestData data, boolean enableUnstableLastVersion) {
      super(ApiKeys.READ_SHARE_GROUP_OFFSETS_STATE, enableUnstableLastVersion);
      this.data = data;
    }

    @Override
    public ReadShareGroupOffsetsStateRequest build(short version) {
      return new ReadShareGroupOffsetsStateRequest(data, version);
    }

    @Override
    public String toString() {
      return data.toString();
    }
  }

  private final ReadShareGroupOffsetsStateRequestData data;

  public ReadShareGroupOffsetsStateRequest(ReadShareGroupOffsetsStateRequestData data, short version) {
    super(ApiKeys.READ_SHARE_GROUP_OFFSETS_STATE, version);
    this.data = data;
  }

  @Override
  public ReadShareGroupOffsetsStateResponse getErrorResponse(int throttleTimeMs, Throwable e) {
    return new ReadShareGroupOffsetsStateResponse(
        new ReadShareGroupOffsetsStateResponseData()
            .setErrorCode(Errors.forException(e).code())
    );
  }

  @Override
  public ReadShareGroupOffsetsStateRequestData data() {
    return data;
  }

  public static ReadShareGroupOffsetsStateRequest parse(ByteBuffer buffer, short version) {
    return new ReadShareGroupOffsetsStateRequest(
        new ReadShareGroupOffsetsStateRequestData(new ByteBufferAccessor(buffer), version),
        version
    );
  }
}
