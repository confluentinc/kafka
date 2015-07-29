# Copyright 2015 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""Commonly used process control signals.

Note that using the python signal module has lead to errors because
it maps signals to integers, and this mapping can be different on
different machines.

Using the signal name seems to be more robust across different Unix/Linux
base operating systems.
"""
SIGTERM = "SIGTERM"
SIGKILL = "SIGKILL"
SIGSTOP = "SIGSTOP"
SIGCONT = "SIGCONT"