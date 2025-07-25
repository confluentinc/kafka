/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.server.common;


import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * This class contains the different Kafka versions.
 * Right now, we use them for upgrades - users can configure the version of the API brokers will use to communicate between themselves.
 * This is only for inter-broker communications - when communicating with clients, the client decides on the API version.
 * <br>
 * Note that the ID we initialize for each version is important.
 * We consider a version newer than another if it is lower in the enum list (to avoid depending on lexicographic order)
 * <br>
 * Since the api protocol may change more than once within the same release and to facilitate people deploying code from
 * trunk, we have the concept of internal versions (first introduced during the 1.0 development cycle). For example,
 * the first time we introduce a version change in a release, say 1.0, we will add a config value "1.0-IV0" and a
 * corresponding enum constant IBP_1_0-IV0. We will also add a config value "1.0" that will be mapped to the
 * latest internal version object, which is IBP_1_0-IV0. When we change the protocol a second time while developing
 * 1.0, we will add a new config value "1.0-IV1" and a corresponding enum constant IBP_1_0-IV1. We will change
 * the config value "1.0" to map to the latest internal version IBP_1_0-IV1. The config value of
 * "1.0-IV0" is still mapped to IBP_1_0-IV0. This way, if people are deploying from trunk, they can use
 * "1.0-IV0" and "1.0-IV1" to upgrade one internal version at a time. For most people who just want to use
 * released version, they can use "1.0" when upgrading to the 1.0 release.
 */
public enum MetadataVersion {

    // Adds InControlledShutdown state to RegisterBrokerRecord and BrokerRegistrationChangeRecord (KIP-841).
    IBP_3_3_IV3(7, "3.3", "IV3", true),

    // Adds ZK to KRaft migration support (KIP-866). This includes ZkMigrationRecord, a new version of RegisterBrokerRecord,
    // and updates to a handful of RPCs.
    IBP_3_4_IV0(8, "3.4", "IV0", true),

    // Support for tiered storage (KIP-405)
    IBP_3_5_IV0(9, "3.5", "IV0", false),

    // Adds replica epoch to Fetch request (KIP-903).
    IBP_3_5_IV1(10, "3.5", "IV1", false),

    // KRaft support for SCRAM
    IBP_3_5_IV2(11, "3.5", "IV2", true),

    // Remove leader epoch bump when KRaft controller shrinks the ISR (KAFKA-15021)
    IBP_3_6_IV0(12, "3.6", "IV0", false),

    // Add metadata transactions
    IBP_3_6_IV1(13, "3.6", "IV1", true),

    // Add KRaft support for Delegation Tokens
    IBP_3_6_IV2(14, "3.6", "IV2", true),

    // Implement KIP-919 controller registration.
    IBP_3_7_IV0(15, "3.7", "IV0", true),

    // Reserved
    IBP_3_7_IV1(16, "3.7", "IV1", false),

    // Add JBOD support for KRaft.
    IBP_3_7_IV2(17, "3.7", "IV2", true),

    // IBP_3_7_IV3 was reserved for ELR support (KIP-966) but has been moved forward to
    // a later release requiring a new MetadataVersion. MVs are not reused.
    IBP_3_7_IV3(18, "3.7", "IV3", false),

    // Add new fetch request version for KIP-951
    IBP_3_7_IV4(19, "3.7", "IV4", false),

    // New version for the Kafka 3.8.0 release.
    IBP_3_8_IV0(20, "3.8", "IV0", false),

    // Support ListOffsetRequest v9 for KIP-1005.
    IBP_3_9_IV0(21, "3.9", "IV0", false),

    // Bootstrap metadata version for version 1 of the GroupVersion feature (KIP-848).
    IBP_4_0_IV0(22, "4.0", "IV0", false),

    // Add ELR related metadata records (KIP-966). Note, ELR is for preview only in 4.0.
    // PartitionRecord and PartitionChangeRecord are updated.
    // ClearElrRecord is added.
    IBP_4_0_IV1(23, "4.0", "IV1", true),

    // Bootstrap metadata version for transaction versions 1 and 2 (KIP-890)
    IBP_4_0_IV2(24, "4.0", "IV2", false),

    // Enables async remote LIST_OFFSETS support (KIP-1075)
    IBP_4_0_IV3(25, "4.0", "IV3", false),

    // Enables ELR by default for new clusters (KIP-966).
    // Share groups are preview in 4.1 (KIP-932).
    // Streams groups are early access in 4.1 (KIP-1071).
    IBP_4_1_IV0(26, "4.1", "IV0", false),

    // Send FETCH version 18 in the replica fetcher (KIP-1166)
    IBP_4_1_IV1(27, "4.1", "IV1", false),

    //
    // NOTE: MetadataVersions after this point are unstable and may be changed.
    // If users attempt to use an unstable MetadataVersion, they will get an error.
    // Please move this comment when updating the LATEST_PRODUCTION constant.
    //

    // Insert any additional IBP_4_1_IVx versions above this comment, and bump the feature level of
    // IBP_4_2_IVx accordingly. When 4.2 development begins, IBP_4_2_IV0 will cease to be
    // a placeholder.

    // Enables share groups by default for new clusters (KIP-932).
    //
    // *** THIS IS A PLACEHOLDER UNSTABLE VERSION WHICH IS USED TO DEFINE THE POINT AT WHICH   ***
    // *** SHARE GROUPS BECOME PRODUCTION-READY IN THE FUTURE. ITS DEFINITION ALLOWS A SHARE   ***
    // *** GROUPS FEATURE TO BE DEFINED IN 4.1 BUT TURNED OFF BY DEFAULT, ABLE TO BE TURNED ON ***
    // *** DYNAMICALLY TO TRY OUT THE PREVIEW CAPABILITY.                                      ***
    IBP_4_2_IV0(28, "4.2", "IV0", false),

    // Enables "streams" groups by default for new clusters (KIP-1071).
    //
    // *** THIS IS A PLACEHOLDER UNSTABLE VERSION WHICH IS USED TO DEFINE THE POINT AT WHICH     ***
    // *** STREAMS GROUPS BECOME PRODUCTION-READY IN THE FUTURE. ITS DEFINITION ALLOWS A STREAMS ***
    // *** GROUPS FEATURE TO BE DEFINED IN 4.1 BUT TURNED OFF BY DEFAULT, ABLE TO BE TURNED ON   ***
    // *** DYNAMICALLY TO TRY OUT THE EARLY ACCESS CAPABILITY.                                   ***
    IBP_4_2_IV1(29, "4.2", "IV1", false);

    // NOTES when adding a new version:
    //   Update the default version in @ClusterTest annotation to point to the latest version
    //   Change expected message in org.apache.kafka.tools.FeatureCommandTest in multiple places (search for "Change expected message")
    public static final String FEATURE_NAME = "metadata.version";

    /**
     * Minimum supported version.
     */
    public static final MetadataVersion MINIMUM_VERSION = IBP_3_3_IV3;

    /**
     * The latest production-ready MetadataVersion. This is the latest version that is stable
     * and cannot be changed. MetadataVersions later than this can be tested via junit, but
     * not deployed in production.
     *
     * <strong>Think carefully before you update this value. ONCE A METADATA VERSION IS PRODUCTION,
     * IT CANNOT BE CHANGED.</strong>
     */
    public static final MetadataVersion LATEST_PRODUCTION = IBP_4_1_IV1;
    // If you change the value above please also update
    // LATEST_STABLE_METADATA_VERSION version in tests/kafkatest/version.py

    /**
     * An array containing all the MetadataVersion entries.
     *
     * This is essentially a cached copy of MetadataVersion.values. Unlike that function, it doesn't
     * allocate a new array each time.
     */
    public static final MetadataVersion[] VERSIONS;

    private final short featureLevel;
    private final String release;
    private final String ibpVersion;
    private final boolean didMetadataChange;

    MetadataVersion(int featureLevel, String release, String subVersion, boolean didMetadataChange) {
        this.featureLevel = (short) featureLevel;
        this.release = release;
        if (subVersion.isEmpty()) {
            this.ibpVersion = release;
        } else {
            this.ibpVersion = String.format("%s-%s", release, subVersion);
        }
        this.didMetadataChange = didMetadataChange;
    }

    public String featureName() {
        return FEATURE_NAME;
    }

    public short featureLevel() {
        return featureLevel;
    }

    public boolean isScramSupported() {
        return this.isAtLeast(IBP_3_5_IV2);
    }

    public boolean isLeaderEpochBumpRequiredOnIsrShrink() {
        return !this.isAtLeast(IBP_3_6_IV0);
    }

    public boolean isMetadataTransactionSupported() {
        return this.isAtLeast(IBP_3_6_IV1);
    }

    public boolean isDelegationTokenSupported() {
        return this.isAtLeast(IBP_3_6_IV2);
    }

    public boolean isDirectoryAssignmentSupported() {
        return this.isAtLeast(IBP_3_7_IV2);
    }

    public boolean isElrSupported() {
        return this.isAtLeast(IBP_4_0_IV1);
    }

    public boolean isMigrationSupported() {
        return this.isAtLeast(MetadataVersion.IBP_3_4_IV0);
    }

    public short registerBrokerRecordVersion() {
        if (isDirectoryAssignmentSupported()) {
            // new logDirs field
            return (short) 3;
        } else if (isMigrationSupported()) {
            // new isMigrationZkBroker field
            return (short) 2;
        } else {
            return (short) 1;
        }
    }

    public short registerControllerRecordVersion() {
        if (isAtLeast(MetadataVersion.IBP_3_7_IV0)) {
            return (short) 0;
        } else {
            throw new RuntimeException("Controller registration is not supported in " +
                    "MetadataVersion " + this);
        }
    }

    public boolean isControllerRegistrationSupported() {
        return this.isAtLeast(MetadataVersion.IBP_3_7_IV0);
    }

    public short partitionChangeRecordVersion() {
        if (isElrSupported()) {
            return (short) 2;
        } else if (isDirectoryAssignmentSupported()) {
            return (short) 1;
        } else {
            return (short) 0;
        }
    }

    public short partitionRecordVersion() {
        if (isElrSupported()) {
            return (short) 2;
        } else if (isDirectoryAssignmentSupported()) {
            return (short) 1;
        } else {
            return (short) 0;
        }
    }

    public short fetchRequestVersion() {
        if (isAtLeast(IBP_4_1_IV1)) {
            return 18;
        } else if (isAtLeast(IBP_3_9_IV0)) {
            return 17;
        } else if (isAtLeast(IBP_3_7_IV4)) {
            return 16;
        } else if (isAtLeast(IBP_3_5_IV1)) {
            return 15;
        } else if (isAtLeast(IBP_3_5_IV0)) {
            return 14;
        } else {
            return 13;
        }
    }

    public short listOffsetRequestVersion() {
        if (this.isAtLeast(IBP_4_0_IV3)) {
            return 10;
        } else if (this.isAtLeast(IBP_3_9_IV0)) {
            return 9;
        } else if (this.isAtLeast(IBP_3_5_IV0)) {
            return 8;
        } else {
            return 7;
        }
    }

    private static final Map<String, MetadataVersion> IBP_VERSIONS;

    static {
        MetadataVersion[] enumValues = MetadataVersion.values();
        VERSIONS = Arrays.copyOf(enumValues, enumValues.length);

        IBP_VERSIONS = new HashMap<>();
        Map<String, MetadataVersion> maxInterVersion = new HashMap<>();
        for (MetadataVersion metadataVersion : VERSIONS) {
            if (metadataVersion.isProduction()) {
                maxInterVersion.put(metadataVersion.release, metadataVersion);
            }
            IBP_VERSIONS.put(metadataVersion.ibpVersion, metadataVersion);
        }
        IBP_VERSIONS.putAll(maxInterVersion);
    }

    public boolean isProduction() {
        return this.compareTo(MetadataVersion.LATEST_PRODUCTION) <= 0;
    }

    public String shortVersion() {
        return release;
    }

    public String version() {
        return ibpVersion;
    }

    public boolean didMetadataChange() {
        return didMetadataChange;
    }

    Optional<MetadataVersion> previous() {
        int idx = this.ordinal();
        if (idx > 0) {
            return Optional.of(VERSIONS[idx - 1]);
        } else {
            return Optional.empty();
        }
    }

    /**
     * Return an `MetadataVersion` instance for `versionString`, which can be in a variety of formats (e.g. "3.8", "3.8.x",
     * "3.8.0", "3.8-IV0"). `IllegalArgumentException` is thrown if `versionString` cannot be mapped to an `MetadataVersion`.
     * Note that 'misconfigured' values such as "3.8.1" will be parsed to `IBP_3_8_IV0` as we ignore anything after the first
     * two segments.
     */
    public static MetadataVersion fromVersionString(String versionString) {
        String[] versionSegments = versionString.split(Pattern.quote("."));
        int numSegments = 2;
        String key;
        if (numSegments >= versionSegments.length) {
            key = versionString;
        } else {
            key = String.join(".", Arrays.copyOfRange(versionSegments, 0, numSegments));
        }
        return Optional.ofNullable(IBP_VERSIONS.get(key)).orElseThrow(() ->
            new IllegalArgumentException("Version " + versionString + " is not a valid version. The minimum version is " + MINIMUM_VERSION
                + " and the maximum version is " + latestTesting())
        );
    }

    public static MetadataVersion fromFeatureLevel(short version) {
        for (MetadataVersion metadataVersion: MetadataVersion.values()) {
            if (metadataVersion.featureLevel() == version) {
                return metadataVersion;
            }
        }
        throw new IllegalArgumentException("No MetadataVersion with feature level " + version + ". Valid feature levels are from "
            + MINIMUM_VERSION.featureLevel + " to " + latestTesting().featureLevel + ".");
    }

    // Testing only
    public static MetadataVersion latestTesting() {
        return VERSIONS[VERSIONS.length - 1];
    }

    public static MetadataVersion latestProduction() {
        return LATEST_PRODUCTION;
    }

    public static boolean checkIfMetadataChanged(MetadataVersion sourceVersion, MetadataVersion targetVersion) {
        if (sourceVersion == targetVersion) {
            return false;
        }

        final MetadataVersion highVersion, lowVersion;
        if (sourceVersion.compareTo(targetVersion) < 0) {
            highVersion = targetVersion;
            lowVersion = sourceVersion;
        } else {
            highVersion = sourceVersion;
            lowVersion = targetVersion;
        }
        return checkIfMetadataChangedOrdered(highVersion, lowVersion);
    }

    private static boolean checkIfMetadataChangedOrdered(MetadataVersion highVersion, MetadataVersion lowVersion) {
        MetadataVersion version = highVersion;
        while (!version.didMetadataChange() && version != lowVersion) {
            Optional<MetadataVersion> prev = version.previous();
            if (prev.isPresent()) {
                version = prev.get();
            } else {
                break;
            }
        }
        return version != lowVersion;
    }

    public boolean isAtLeast(MetadataVersion otherVersion) {
        return this.compareTo(otherVersion) >= 0;
    }

    public boolean isLessThan(MetadataVersion otherVersion) {
        return this.compareTo(otherVersion) < 0;
    }

    @Override
    public String toString() {
        return ibpVersion;
    }
}
