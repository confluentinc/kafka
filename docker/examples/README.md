Kafka Docker Image Usage Guide
==============================

Introduction
------------

This document contains usage guide as well as examples for Docker image.
Docker Compose files are provided in this directory for the example use cases.

Kafka server can be started using following ways:

- Default configs
- File input
- Environment variables

Installation Preparation
------------

Note that the `Docker` version **must be >= 20.10.4**.

The prior Docker versions may cause permission errors when running the Kafka container, as they do not correctly set directory permissions when creating container paths like `/opt/kafka/config`.

If you are using the prior version, you may encounter the following error during container startup:
```text
===> User
uid=1000(appuser) gid=1000(appuser) groups=1000(appuser)
===> Setting default values of environment variables if not already set.
===> Configuring …
Running in KRaft mode…
/opt/kafka/config/ file not writable
```

To avoid this, **please upgrade Docker to 20.10.4 or later**.

Running on default configs
--------------------------

If no user provided configuration (file input or environment variables) is passed to the Docker container, the default KRaft configuration for single combined-mode node will be used.
This default configuration is packaged in the Kafka tarball.

Use input file for providing configs 
------------------------------------

- This method requires users to provide path to a local folder which contains the Kafka property files and mount it to Docker container using Docker volume.
- It replaces the default KRaft configuration file present in Docker container.
- The Command `docker run --volume /path/to/property/folder:/mnt/shared/config -p 9092:9092 apache/kafka:latest` can be used to mount the folder containing the property files.
- Property files will be only read by the Docker container.

Using Environment Variables
---------------------------

When using the environment variables, you need to set all properties required to start the KRaft node.
Therefore, the recommended way to use environment variables is via Docker Compose, which allows users to set all the properties that are needed.
It is also possible to use the input file to have a common set of configurations, and then override specific node properties using the environment variables.

- Kafka property defined via environment variables will override the value of that property defined in the user provided property file.
- If properties are provided via environment variables only, all required properties must be specified.
- The following rules must be used to construct the environment variable key name:
    - Replace `.` with `_`
    - Replace `_` with `__` (double underscore)
    - Replace `-` with `___` (triple underscore)
    - Prefix the result with `KAFKA_`
    - Examples:
        - For `abc.def`, use `KAFKA_ABC_DEF`
        - For `abc-def`, use `KAFKA_ABC___DEF`
        - For `abc_def`, use `KAFKA_ABC__DEF`
        
- To provide configs to log4j property files, following points should be considered:
  - log4j properties provided via environment variables will be appended to the default properties file (log4j properties files bundled with Kafka).
  - `KAFKA_LOG4J_ROOT_LOGLEVEL` can be provided to set the value of `log4j.rootLogger` in `log4j2.yaml` and `tools-log4j2.yaml`.
  - log4j loggers can be added to `log4j2.yaml` by setting them in `KAFKA_LOG4J_LOGGERS` environment variable in a single comma separated string.
      - Example:
          - Assuming that `KAFKA_LOG4J_LOGGERS='property1=value1,property2=value2'` environment variable is provided to Docker container.
          - `log4j.logger.property1=value1` and `log4j.logger.property2=value2` will be added to the `log4j2.yaml` file inside Docker container.

Running in SSL mode
-------------------
        
- Recommended way to run in ssl mode is by mounting secrets on `/etc/kafka/secrets` in Docker container and providing configs following through environment variables (`KAFKA_SSL_KEYSTORE_FILENAME`, `KAFKA_SSL_KEYSTORE_CREDENTIALS`, `KAFKA_SSL_KEY_CREDENTIALS`, `KAFKA_SSL_TRUSTSTORE_FILENAME` and `KAFKA_SSL_TRUSTSTORE_CREDENTIALS`) to let the Docker image scripts extract passwords and populate correct paths in `server.properties`.      
- Please ensure appropriate `KAFKA_ADVERTISED_LISTENERS` are provided through environment variables to enable SSL mode in Kafka server, i.e. it should contain an `SSL` listener.
- Alternatively property file input can be used to provide ssl properties.
- Make sure you set location of truststore and keystore correctly when using file input. See example for file input in `docker-compose-files/single-node/file-input` for better clarity.
- Note that advertised.listeners property needs to be provided along with SSL properties in file input and cannot be provided through environment variable separately.
- In conclusion, ssl properties with advertised.listeners should be treated as a group and provided in file input or environment variables in it's entirety.
- In case ssl properties are provided both through file input and environment variables, environment variable properties will override the file input properties, just as mentioned in the beginning of this section.

Examples
--------

- `docker-compose-files` directory contains Docker Compose files for some example configs to run `apache/kafka` OR `apache/kafka-native` Docker image.
- Pass the `IMAGE` variable with the Docker Compose file to specify which Docker image to use for bringing up the containers.
```
# to bring up containers using apache/kafka docker image
IMAGE=apache/kafka:latest <docker compose command>

# to bring up containers using apache/kafka-native docker image
IMAGE=apache/kafka-native:latest <docker compose command>
```
- Run the commands from root of the repository.
- Checkout `single-node` examples for quick small examples to play around with.
- `cluster` contains multi node examples, for `combined` mode as well as `isolated` mode.
- Kafka server running on Docker container can be accessed using cli scripts or your own client code.
- Make sure jars are built, if you decide to use cli scripts of this repo.

Single Node
-----------

- These examples are for understanding various ways inputs can be provided and kafka can be configured in Docker container.
- Examples are present inside `docker-compose-files/single-node` directory.
- Plaintext:
    - This is the simplest compose file.
    - We are using environment variables purely for providing configs.
    - `KAFKA_LISTENERS` is getting supplied. But if it was not provided, defaulting would have kicked in and we would have used `KAFKA_ADVERTISED_LISTENERS` to generate `KAFKA_LISTENERS`, by replacing the host with `0.0.0.0`.
    - Note that we have provided a `CLUSTER_ID`, but it's not mandatory as there is a default cluster id present in container.
    - We had to provide `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` and set it explicitly to 1, because if we don't provide it default value provided by kafka will be taken which is 3.
    - We have also set hostname of the container.
      It can be kept same as the container name for clarity.
    - To run the example:
    ```
    # Run from root of the repo
  
    # JVM based Apache Kafka Docker Image
    $ IMAGE=apache/kafka:latest docker compose -f docker/examples/docker-compose-files/single-node/plaintext/docker-compose.yml up
  
    # GraalVM based Native Apache Kafka Docker Image
    $ IMAGE=apache/kafka-native:latest docker compose -f docker/examples/docker-compose-files/single-node/plaintext/docker-compose.yml up
    ```
    - To produce messages using client scripts:
    ```
    # Run from root of the repo
    $ bin/kafka-console-producer.sh --topic test --bootstrap-server localhost:9092
    ```
- SSL:
    - Note that here we are using environment variables to pass configs.
    - Notice how secrets folder is mounted to docker container.
    - In case of environment variable it is mandatory to keep the files in `/etc/kafka/secrets` folder in docker container, given that the path of the files will be derived from that, as we are just providing file names in other SSL configs.
    - To run the example:
    ```
    # Run from root of the repo
    
    # JVM based Apache Kafka Docker Image
    $ IMAGE=apache/kafka:latest docker compose -f docker/examples/docker-compose-files/single-node/ssl/docker-compose.yml up

    # GraalVM based Native Apache Kafka Docker Image
    $ IMAGE=apache/kafka-native:latest docker compose -f docker/examples/docker-compose-files/single-node/ssl/docker-compose.yml up
    ```
    - To produce messages using client scripts (Ensure that java version >= 17):
    ```
    # Run from root of the repo
    $ bin/kafka-console-producer.sh --topic test --bootstrap-server localhost:9093 --producer.config ./docker/examples/fixtures/client-secrets/client-ssl.properties
    ```
- File Input:
    - Here ssl configs are provided via file input.
    - Notice that now full file path is needed for the configs.
    - Note that there is extra volume mount now.
    - Configs provided via environment variable will override the file input configs.
    - To run the example:
    ```
    # Run from root of the repo
  
    # JVM based Apache Kafka Docker Image
    $ IMAGE=apache/kafka:latest docker compose -f docker/examples/docker-compose-files/single-node/file-input/docker-compose.yml up
  
    # GraalVM based Native Apache Kafka Docker Image
    $ IMAGE=apache/kafka-native:latest docker compose -f docker/examples/docker-compose-files/single-node/file-input/docker-compose.yml up
    ```
    - To produce messages using client scripts (Ensure that java version >= 17):
    ```
    # Run from root of the repo
    $ bin/kafka-console-producer.sh --topic test --bootstrap-server localhost:9093 --producer.config ./docker/examples/fixtures/client-secrets/client-ssl.properties
    ```

Multi Node Cluster
------------------

- These examples are for real world use cases where multiple nodes of kafka are required.
- Combined:
    - Examples are present in `docker-compose-files/cluster/combined` directory.
    - Plaintext:
        - Each broker must expose a unique port to host machine.
            - For example broker-1, broker2 and broker3 are listening on port 9092, they're exposing it to the host via ports 29092, 39092 and 49092 respectively.
        - Here important thing to note is that to ensure that kafka brokers are accessible both to clients as well as to each other we have introduced an additional listener.
        - PLAINTEXT is supposed to be listener accessible to other brokers.
            - The inter broker listener advertised by the brokers is exposed on container's hostname. This is done so that brokers can find each other in Docker network.
        - PLAINTEXT_HOST is supposed to be listener accessible to the clients.
            - The port advertised for host machine is done on localhost, as this is the hostname (in this example) that client will use to connect with kafka running inside Docker container.
        - Here we take advantage of hostname set for each broker and set the listener accordingly.
        - To run the example:
        ```
        # Run from root of the repo
      
        # JVM based Apache Kafka Docker Image
        $ IMAGE=apache/kafka:latest docker compose -f docker/examples/docker-compose-files/cluster/combined/plaintext/docker-compose.yml up
      
        # GraalVM based Native Apache Kafka Docker Image
        $ IMAGE=apache/kafka-native:latest docker compose -f docker/examples/docker-compose-files/cluster/combined/plaintext/docker-compose.yml up
        ```
        - To access using client script:
        ```
        # Run from root of the repo
        $ bin/kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:29092
        ```
    - SSL:
        - Similar to Plaintext example, for inter broker communication in SSL mode, SSL-INTERNAL is required and for client to broker communication, SSL is required.
        - Note that `KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM` is set to empty as hostname was not set in credentials.
          This won't be needed in production use cases.
        - Also note that for example we have used the same credentials for all brokers.
          Make sure each broker has it's own secrets.
        - To run the example:
        ```
        # Run from root of the repo
      
        # JVM based Apache Kafka Docker Image
        $ IMAGE=apache/kafka:latest docker compose -f docker/examples/docker-compose-files/cluster/combined/ssl/docker-compose.yml up
      
        # GraalVM based Native Apache Kafka Docker Image
        $ IMAGE=apache/kafka-native:latest docker compose -f docker/examples/docker-compose-files/cluster/combined/ssl/docker-compose.yml up
        ```
        - To produce messages using client scripts (Ensure that java version >= 17):
        ```
        # Run from root of the repo
        $ bin/kafka-console-producer.sh --topic test --bootstrap-server localhost:29093 --producer.config ./docker/examples/fixtures/client-secrets/client-ssl.properties
        ```
- Isolated:
    - Examples are present in `docker-compose-files/cluster/isolated` directory.
    - Plaintext:
        - Here controllers and brokers are configured separately.
        - It's a good practice to define that brokers depend on controllers.
        - In this case also we have same listeners setup as mentioned in combined case.
        - To run the example:
        ```
        # Run from root of the repo
      
        # JVM based Apache Kafka Docker Image
        $ IMAGE=apache/kafka:latest docker compose -f docker/examples/docker-compose-files/cluster/isolated/plaintext/docker-compose.yml up
      
        # GraalVM based Native Apache Kafka Docker Image
        $ IMAGE=apache/kafka-native:latest docker compose -f docker/examples/docker-compose-files/cluster/isolated/plaintext/docker-compose.yml up
        ```
        - To access using client script:
        ```
        # Run from root of the repo
        $ bin/kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:29092
        ```
    - SSL:
        - Pretty much same as combined example, with controllers and brokers separated now.
        - Note that `SSL-INTERNAL` is only for inter broker communication and controllers are using `PLAINTEXT`.
        - To run the example:
        ```
        # Run from root of the repo
      
        # JVM based Apache Kafka Docker Image
        $ IMAGE=apache/kafka:latest docker compose -f docker/examples/docker-compose-files/cluster/isolated/ssl/docker-compose.yml up
      
        # GraalVM based Native Apache Kafka Docker Image
        $ IMAGE=apache/kafka-native:latest docker compose -f docker/examples/docker-compose-files/cluster/isolated/ssl/docker-compose.yml up
        ```
        - To produce messages using client scripts (Ensure that java version >= 17):
        ```
        # Run from root of the repo
        $ bin/kafka-console-producer.sh --topic test --bootstrap-server localhost:29093 --producer.config ./docker/examples/fixtures/client-secrets/client-ssl.properties
        ```

- Note that the examples are meant to be tried one at a time, make sure you close an example server before trying out the other to avoid conflicts.
