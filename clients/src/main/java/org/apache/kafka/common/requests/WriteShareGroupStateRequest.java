package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.WriteShareGroupStateRequestData;
import org.apache.kafka.common.message.WriteShareGroupStateResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class WriteShareGroupStateRequest extends AbstractRequest {
  public static class Builder extends AbstractRequest.Builder<WriteShareGroupStateRequest> {

    private final WriteShareGroupStateRequestData data;

    public Builder(WriteShareGroupStateRequestData data) {
      this(data, false);
    }

    public Builder(WriteShareGroupStateRequestData data, boolean enableUnstableLastVersion) {
      super(ApiKeys.WRITE_SHARE_GROUP_STATE, enableUnstableLastVersion);
      this.data = data;
    }

    @Override
    public WriteShareGroupStateRequest build(short version) {
      return new WriteShareGroupStateRequest(data, version);
    }

    @Override
    public String toString() {
      return data.toString();
    }
  }

  private final WriteShareGroupStateRequestData data;

  public WriteShareGroupStateRequest(WriteShareGroupStateRequestData data, short version) {
    super(ApiKeys.WRITE_SHARE_GROUP_STATE, version);
    this.data = data;
  }

  @Override
  public WriteShareGroupStateResponse getErrorResponse(int throttleTimeMs, Throwable e) {
    return new WriteShareGroupStateResponse(
        new WriteShareGroupStateResponseData()
            .setErrorCode(Errors.forException(e).code())
    );
  }

  @Override
  public WriteShareGroupStateRequestData data() {
    return data;
  }

  public static WriteShareGroupStateRequest parse(ByteBuffer buffer, short version) {
    return new WriteShareGroupStateRequest(
        new WriteShareGroupStateRequestData(new ByteBufferAccessor(buffer), version),
        version
    );
  }
}
