package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.DeleteShareGroupStateRequestData;
import org.apache.kafka.common.message.DeleteShareGroupStateResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class DeleteShareGroupStateRequest extends AbstractRequest {
  public static class Builder extends AbstractRequest.Builder<DeleteShareGroupStateRequest> {

    private final DeleteShareGroupStateRequestData data;

    public Builder(DeleteShareGroupStateRequestData data) {
      this(data, false);
    }

    public Builder(DeleteShareGroupStateRequestData data, boolean enableUnstableLastVersion) {
      super(ApiKeys.DELETE_SHARE_GROUP_STATE, enableUnstableLastVersion);
      this.data = data;
    }

    @Override
    public DeleteShareGroupStateRequest build(short version) {
      return new DeleteShareGroupStateRequest(data, version);
    }

    @Override
    public String toString() {
      return data.toString();
    }
  }

  private final DeleteShareGroupStateRequestData data;

  public DeleteShareGroupStateRequest(DeleteShareGroupStateRequestData data, short version) {
    super(ApiKeys.DELETE_SHARE_GROUP_STATE, version);
    this.data = data;
  }

  @Override
  public DeleteShareGroupStateResponse getErrorResponse(int throttleTimeMs, Throwable e) {
    return new DeleteShareGroupStateResponse(
        new DeleteShareGroupStateResponseData()
            .setErrorCode(Errors.forException(e).code())
    );
  }

  @Override
  public DeleteShareGroupStateRequestData data() {
    return data;
  }

  public static DeleteShareGroupStateRequest parse(ByteBuffer buffer, short version) {
    return new DeleteShareGroupStateRequest(
        new DeleteShareGroupStateRequestData(new ByteBufferAccessor(buffer), version),
        version
    );
  }
}
