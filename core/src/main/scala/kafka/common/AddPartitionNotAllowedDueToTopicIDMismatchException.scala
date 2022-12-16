package kafka.common

class AddPartitionNotAllowedDueToTopicIDMismatchException(message: String) extends RuntimeException(message) {
  def this() = this(null)
}
