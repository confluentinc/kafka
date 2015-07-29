This package contains small tests to sanity check the functionality of services like KafkaService.

When writing system or integration tests, it is helpful to know that the Service subclasses on which you rely
work the way you think they work. Therefore, it is useful to have a class of tests which only exercise
small facets of your Service subclasses. Such tests go in this directory, and can be run using ducktape just
like the system tests.