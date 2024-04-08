package org.apache.kafka.server.group.share;

import org.apache.kafka.common.Uuid;

import java.util.List;
import java.util.Objects;

public class TopicData {
  private final Uuid topicId;
  private final List<PartitionData> partitions;

  public TopicData(Uuid topicId, List<PartitionData> partitions) {
    this.topicId = topicId;
    this.partitions = partitions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TopicData topicData = (TopicData) o;
    return Objects.equals(topicId, topicData.topicId) && Objects.equals(partitions, topicData.partitions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topicId, partitions);
  }

  public Uuid topicId() {
    return topicId;
  }

  public List<PartitionData> partitions() {
    return partitions;
  }
}
