#!/bin/bash

declare -A kafkaMuckrakeVersionMap

kafkaMuckrakeVersionMap["2.3"]="5.3.x"
kafkaMuckrakeVersionMap["2.4"]="5.4.x"
kafkaMuckrakeVersionMap["2.5"]="5.5.x"
kafkaMuckrakeVersionMap["2.6"]="5.6.x"
kafkaMuckrakeVersionMap["2.7"]="5.7.x"
kafkaMuckrakeVersionMap["2.8"]="5.8.x"
kafkaMuckrakeVersionMap["3.0"]="6.0.x"
kafkaMuckrakeVersionMap["3.1"]="6.1.x"
kafkaMuckrakeVersionMap["3.2"]="6.2.x"
kafkaMuckrakeVersionMap["3.3"]="6.3.x"
kafkaMuckrakeVersionMap["3.4"]="6.4.x"
kafkaMuckrakeVersionMap["3.5"]="6.5.x"
kafkaMuckrakeVersionMap["3.6"]="6.6.x"
kafkaMuckrakeVersionMap["3.7"]="6.7.x"
kafkaMuckrakeVersionMap["trunk"]="master"
kafkaMuckrakeVersionMap["master"]="master"

a=key1
# Step 3: Access a value using its key
echo "The value for 'key1' is: ${kafkaMuckrakeVersionMap["${a}"]}"
echo "The value for 'key2' is: ${kafkaMuckrakeVersionMap["key2"]}"