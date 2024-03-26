package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.InitializeShareGroupStateRequestData;
import org.apache.kafka.common.message.InitializeShareGroupStateResponseData;
import org.apache.kafka.common.message.ShareGroupDescribeRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class InitializeShareGroupStateRequest extends AbstractRequest {
  public static class Builder extends AbstractRequest.Builder<InitializeShareGroupStateRequest> {

    private final InitializeShareGroupStateRequestData data;

    public Builder(InitializeShareGroupStateRequestData data) {
      this(data, false);
    }

    public Builder(InitializeShareGroupStateRequestData data, boolean enableUnstableLastVersion) {
      super(ApiKeys.INITIALIZE_SHARE_GROUP_STATE, enableUnstableLastVersion);
      this.data = data;
    }

    @Override
    public InitializeShareGroupStateRequest build(short version) {
      return new InitializeShareGroupStateRequest(data, version);
    }

    @Override
    public String toString() {
      return data.toString();
    }
  }

  private final InitializeShareGroupStateRequestData data;

  public InitializeShareGroupStateRequest(InitializeShareGroupStateRequestData data, short version) {
    super(ApiKeys.INITIALIZE_SHARE_GROUP_STATE, version);
    this.data = data;
  }

  @Override
  public InitializeShareGroupStateResponse getErrorResponse(int throttleTimeMs, Throwable e) {
    return new InitializeShareGroupStateResponse(
        new InitializeShareGroupStateResponseData()
            .setErrorCode(Errors.forException(e).code())
    );
  }

  @Override
  public InitializeShareGroupStateRequestData data() {
    return data;
  }

  public static ShareGroupDescribeRequest parse(ByteBuffer buffer, short version) {
    return new ShareGroupDescribeRequest(
        new ShareGroupDescribeRequestData(new ByteBufferAccessor(buffer), version),
        version
    );
  }
}
