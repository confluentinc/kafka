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
package org.apache.kafka.streams.state.internals;

import org.rocksdb.AbstractCompactionFilter;
import org.rocksdb.AbstractCompactionFilterFactory;
import org.rocksdb.AbstractComparator;
import org.rocksdb.AbstractEventListener;
import org.rocksdb.AbstractSlice;
import org.rocksdb.AbstractWalFilter;
import org.rocksdb.BuiltinComparator;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionOptionsFIFO;
import org.rocksdb.CompactionOptionsUniversal;
import org.rocksdb.CompactionPriority;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.ConcurrentTaskLimiter;
import org.rocksdb.DBOptions;
import org.rocksdb.DbPath;
import org.rocksdb.Env;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.LoggerInterface;
import org.rocksdb.MemTableConfig;
import org.rocksdb.MergeOperator;
import org.rocksdb.Options;
import org.rocksdb.PrepopulateBlobCache;
import org.rocksdb.RateLimiter;
import org.rocksdb.SstFileManager;
import org.rocksdb.SstPartitionerFactory;
import org.rocksdb.Statistics;
import org.rocksdb.TableFormatConfig;
import org.rocksdb.WALRecoveryMode;
import org.rocksdb.WalFilter;
import org.rocksdb.WriteBufferManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

/**
 * The generic {@link Options} class allows users to set all configs on one object if only default column family
 * is used. Because we use multiple column families, we need to use {@link DBOptions} and {@link ColumnFamilyOptions}
 * that cover a part of all options each.
 *
 * This class do the translation between generic {@link Options} into {@link DBOptions} and {@link ColumnFamilyOptions}.
 */
public class RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter extends Options {

    private static final Logger log = LoggerFactory.getLogger(RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter.class);

    private final DBOptions dbOptions;
    private final ColumnFamilyOptions columnFamilyOptions;

    RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter(final DBOptions dbOptions,
                                                               final ColumnFamilyOptions columnFamilyOptions) {
        this.dbOptions = dbOptions;
        this.columnFamilyOptions = columnFamilyOptions;
    }

    @Override
    public Options setIncreaseParallelism(final int totalThreads) {
        dbOptions.setIncreaseParallelism(totalThreads);
        return this;
    }

    @Override
    public Options setCreateIfMissing(final boolean flag) {
        dbOptions.setCreateIfMissing(flag);
        return this;
    }

    @Override
    public Options setCreateMissingColumnFamilies(final boolean flag) {
        dbOptions.setCreateMissingColumnFamilies(flag);
        return this;
    }

    @Override
    public Options setEnv(final Env env) {
        dbOptions.setEnv(env);
        return this;
    }

    @Override
    public Env getEnv() {
        return dbOptions.getEnv();
    }

    @Override
    public Options prepareForBulkLoad() {
        super.prepareForBulkLoad();
        return this;
    }

    @Override
    public boolean createIfMissing() {
        return dbOptions.createIfMissing();
    }

    @Override
    public boolean createMissingColumnFamilies() {
        return dbOptions.createMissingColumnFamilies();
    }

    @Override
    public Options optimizeForSmallDb() {
        dbOptions.optimizeForSmallDb();
        columnFamilyOptions.optimizeForSmallDb();
        return this;
    }

    @Override
    public Options optimizeForPointLookup(final long blockCacheSizeMb) {
        columnFamilyOptions.optimizeForPointLookup(blockCacheSizeMb);
        return this;
    }

    @Override
    public Options optimizeLevelStyleCompaction() {
        columnFamilyOptions.optimizeLevelStyleCompaction();
        return this;
    }

    @Override
    public Options optimizeLevelStyleCompaction(final long memtableMemoryBudget) {
        columnFamilyOptions.optimizeLevelStyleCompaction(memtableMemoryBudget);
        return this;
    }

    @Override
    public Options optimizeUniversalStyleCompaction() {
        columnFamilyOptions.optimizeUniversalStyleCompaction();
        return this;
    }

    @Override
    public Options optimizeUniversalStyleCompaction(final long memtableMemoryBudget) {
        columnFamilyOptions.optimizeUniversalStyleCompaction(memtableMemoryBudget);
        return this;
    }

    @Override
    public Options setComparator(final BuiltinComparator builtinComparator) {
        columnFamilyOptions.setComparator(builtinComparator);
        return this;
    }

    @Override
    public Options setComparator(final AbstractComparator comparator) {
        columnFamilyOptions.setComparator(comparator);
        return this;
    }

    @Override
    public Options setMergeOperatorName(final String name) {
        columnFamilyOptions.setMergeOperatorName(name);
        return this;
    }

    @Override
    public Options setMergeOperator(final MergeOperator mergeOperator) {
        columnFamilyOptions.setMergeOperator(mergeOperator);
        return this;
    }

    @Override
    public Options setWriteBufferSize(final long writeBufferSize) {
        columnFamilyOptions.setWriteBufferSize(writeBufferSize);
        return this;
    }

    @Override
    public long writeBufferSize()  {
        return columnFamilyOptions.writeBufferSize();
    }

    @Override
    public Options setMaxWriteBufferNumber(final int maxWriteBufferNumber) {
        columnFamilyOptions.setMaxWriteBufferNumber(maxWriteBufferNumber);
        return this;
    }

    @Override
    public int maxWriteBufferNumber() {
        return columnFamilyOptions.maxWriteBufferNumber();
    }

    @Override
    public boolean errorIfExists() {
        return dbOptions.errorIfExists();
    }

    @Override
    public Options setErrorIfExists(final boolean errorIfExists) {
        dbOptions.setErrorIfExists(errorIfExists);
        return this;
    }

    @Override
    public boolean paranoidChecks() {
        final boolean columnFamilyParanoidFileChecks = columnFamilyOptions.paranoidFileChecks();
        final boolean dbOptionsParanoidChecks = dbOptions.paranoidChecks();

        if (columnFamilyParanoidFileChecks != dbOptionsParanoidChecks) {
            throw new IllegalStateException("Config for paranoid checks for RockDB and ColumnFamilies should be the same.");
        }

        return dbOptionsParanoidChecks;
    }

    @Override
    public Options setParanoidChecks(final boolean paranoidChecks) {
        columnFamilyOptions.paranoidFileChecks();
        dbOptions.setParanoidChecks(paranoidChecks);
        return this;
    }

    @Override
    public int maxOpenFiles() {
        return dbOptions.maxOpenFiles();
    }

    @Override
    public Options setMaxFileOpeningThreads(final int maxFileOpeningThreads) {
        dbOptions.setMaxFileOpeningThreads(maxFileOpeningThreads);
        return this;
    }

    @Override
    public int maxFileOpeningThreads() {
        return dbOptions.maxFileOpeningThreads();
    }

    @Override
    public Options setMaxTotalWalSize(final long maxTotalWalSize) {
        logIgnoreWalOption("maxTotalWalSize");
        return this;
    }

    @Override
    public long maxTotalWalSize() {
        return dbOptions.maxTotalWalSize();
    }

    @Override
    public Options setMaxOpenFiles(final int maxOpenFiles) {
        dbOptions.setMaxOpenFiles(maxOpenFiles);
        return this;
    }

    @Override
    public boolean useFsync() {
        return dbOptions.useFsync();
    }

    @Override
    public Options setUseFsync(final boolean useFsync) {
        dbOptions.setUseFsync(useFsync);
        return this;
    }

    @Override
    public Options setDbPaths(final Collection<DbPath> dbPaths) {
        dbOptions.setDbPaths(dbPaths);
        return this;
    }

    @Override
    public List<DbPath> dbPaths() {
        return dbOptions.dbPaths();
    }

    @Override
    public String dbLogDir() {
        return dbOptions.dbLogDir();
    }

    @Override
    public Options setDbLogDir(final String dbLogDir) {
        dbOptions.setDbLogDir(dbLogDir);
        return this;
    }

    @Override
    public String walDir() {
        return dbOptions.walDir();
    }

    @Override
    public Options setWalDir(final String walDir) {
        logIgnoreWalOption("walDir");
        return this;
    }

    @Override
    public long deleteObsoleteFilesPeriodMicros() {
        return dbOptions.deleteObsoleteFilesPeriodMicros();
    }

    @Override
    public Options setDeleteObsoleteFilesPeriodMicros(final long micros) {
        dbOptions.setDeleteObsoleteFilesPeriodMicros(micros);
        return this;
    }

    @Override
    public Options setStatistics(final Statistics statistics) {
        dbOptions.setStatistics(statistics);
        return this;
    }

    @Override
    public Statistics statistics() {
        return dbOptions.statistics();
    }

    @Override
    public Options setMaxSubcompactions(final int maxSubcompactions) {
        dbOptions.setMaxSubcompactions(maxSubcompactions);
        return this;
    }

    @Override
    public int maxSubcompactions() {
        return dbOptions.maxSubcompactions();
    }

    @Override
    public int maxBackgroundJobs() {
        return dbOptions.maxBackgroundJobs();
    }

    @Override
    public Options setMaxBackgroundJobs(final int maxBackgroundJobs) {
        dbOptions.setMaxBackgroundJobs(maxBackgroundJobs);
        return this;
    }

    @Override
    public long maxLogFileSize() {
        return dbOptions.maxLogFileSize();
    }

    @Override
    public Options setMaxLogFileSize(final long maxLogFileSize) {
        dbOptions.setMaxLogFileSize(maxLogFileSize);
        return this;
    }

    @Override
    public long logFileTimeToRoll() {
        return dbOptions.logFileTimeToRoll();
    }

    @Override
    public Options setLogFileTimeToRoll(final long logFileTimeToRoll) {
        dbOptions.setLogFileTimeToRoll(logFileTimeToRoll);
        return this;
    }

    @Override
    public long keepLogFileNum() {
        return dbOptions.keepLogFileNum();
    }

    @Override
    public Options setKeepLogFileNum(final long keepLogFileNum) {
        dbOptions.setKeepLogFileNum(keepLogFileNum);
        return this;
    }

    @Override
    public Options setRecycleLogFileNum(final long recycleLogFileNum) {
        dbOptions.setRecycleLogFileNum(recycleLogFileNum);
        return this;
    }

    @Override
    public long recycleLogFileNum() {
        return dbOptions.recycleLogFileNum();
    }

    @Override
    public long maxManifestFileSize() {
        return dbOptions.maxManifestFileSize();
    }

    @Override
    public Options setMaxManifestFileSize(final long maxManifestFileSize) {
        dbOptions.setMaxManifestFileSize(maxManifestFileSize);
        return this;
    }

    @Override
    public Options setMaxTableFilesSizeFIFO(final long maxTableFilesSize) {
        columnFamilyOptions.setMaxTableFilesSizeFIFO(maxTableFilesSize);
        return this;
    }

    @Override
    public long maxTableFilesSizeFIFO() {
        return columnFamilyOptions.maxTableFilesSizeFIFO();
    }

    @Override
    public int tableCacheNumshardbits() {
        return dbOptions.tableCacheNumshardbits();
    }

    @Override
    public Options setTableCacheNumshardbits(final int tableCacheNumshardbits) {
        dbOptions.setTableCacheNumshardbits(tableCacheNumshardbits);
        return this;
    }

    @Override
    public long walTtlSeconds() {
        return dbOptions.walTtlSeconds();
    }

    @Override
    public Options setWalTtlSeconds(final long walTtlSeconds) {
        logIgnoreWalOption("walTtlSeconds");
        return this;
    }

    @Override
    public long walSizeLimitMB() {
        return dbOptions.walSizeLimitMB();
    }

    @Override
    public Options setWalSizeLimitMB(final long sizeLimitMB) {
        logIgnoreWalOption("walSizeLimitMB");
        return this;
    }

    @Override
    public long manifestPreallocationSize() {
        return dbOptions.manifestPreallocationSize();
    }

    @Override
    public Options setManifestPreallocationSize(final long size) {
        dbOptions.setManifestPreallocationSize(size);
        return this;
    }

    @Override
    public Options setUseDirectReads(final boolean useDirectReads) {
        dbOptions.setUseDirectReads(useDirectReads);
        return this;
    }

    @Override
    public boolean useDirectReads() {
        return dbOptions.useDirectReads();
    }

    @Override
    public Options setUseDirectIoForFlushAndCompaction(final boolean useDirectIoForFlushAndCompaction) {
        dbOptions.setUseDirectIoForFlushAndCompaction(useDirectIoForFlushAndCompaction);
        return this;
    }

    @Override
    public boolean useDirectIoForFlushAndCompaction() {
        return dbOptions.useDirectIoForFlushAndCompaction();
    }

    @Override
    public Options setAllowFAllocate(final boolean allowFAllocate) {
        dbOptions.setAllowFAllocate(allowFAllocate);
        return this;
    }

    @Override
    public boolean allowFAllocate() {
        return dbOptions.allowFAllocate();
    }

    @Override
    public boolean allowMmapReads() {
        return dbOptions.allowMmapReads();
    }

    @Override
    public Options setAllowMmapReads(final boolean allowMmapReads) {
        dbOptions.setAllowMmapReads(allowMmapReads);
        return this;
    }

    @Override
    public boolean allowMmapWrites() {
        return dbOptions.allowMmapWrites();
    }

    @Override
    public Options setAllowMmapWrites(final boolean allowMmapWrites) {
        dbOptions.setAllowMmapWrites(allowMmapWrites);
        return this;
    }

    @Override
    public boolean isFdCloseOnExec() {
        return dbOptions.isFdCloseOnExec();
    }

    @Override
    public Options setIsFdCloseOnExec(final boolean isFdCloseOnExec) {
        dbOptions.setIsFdCloseOnExec(isFdCloseOnExec);
        return this;
    }

    @Override
    public int statsDumpPeriodSec() {
        return dbOptions.statsDumpPeriodSec();
    }

    @Override
    public Options setStatsDumpPeriodSec(final int statsDumpPeriodSec) {
        dbOptions.setStatsDumpPeriodSec(statsDumpPeriodSec);
        return this;
    }

    @Override
    public boolean adviseRandomOnOpen() {
        return dbOptions.adviseRandomOnOpen();
    }

    @Override
    public Options setAdviseRandomOnOpen(final boolean adviseRandomOnOpen) {
        dbOptions.setAdviseRandomOnOpen(adviseRandomOnOpen);
        return this;
    }

    @Override
    public Options setDbWriteBufferSize(final long dbWriteBufferSize) {
        dbOptions.setDbWriteBufferSize(dbWriteBufferSize);
        return this;
    }

    @Override
    public long dbWriteBufferSize() {
        return dbOptions.dbWriteBufferSize();
    }

    @Override
    public Options setCompactionReadaheadSize(final long compactionReadaheadSize) {
        dbOptions.setCompactionReadaheadSize(compactionReadaheadSize);
        return this;
    }

    @Override
    public long compactionReadaheadSize() {
        return dbOptions.compactionReadaheadSize();
    }

    @Deprecated(since = "4.2", forRemoval = true)
    public Options setRandomAccessMaxBufferSize(final long ignored) {
        log.warn("random_access_max_buffer_size has been removed in RocksDB v9.11.1." +
                " See https://github.com/facebook/rocksdb/pull/13288");
        return this;
    }

    @Deprecated(since = "4.2", forRemoval = true)
    public long randomAccessMaxBufferSize() {
        log.warn("random_access_max_buffer_size has been removed in RocksDB v9.11.1." +
                " See https://github.com/facebook/rocksdb/pull/13288");
        return 0;
    }

    @Override
    public Options setWritableFileMaxBufferSize(final long writableFileMaxBufferSize) {
        dbOptions.setWritableFileMaxBufferSize(writableFileMaxBufferSize);
        return this;
    }

    @Override
    public long writableFileMaxBufferSize() {
        return dbOptions.writableFileMaxBufferSize();
    }

    @Override
    public boolean useAdaptiveMutex() {
        return dbOptions.useAdaptiveMutex();
    }

    @Override
    public Options setUseAdaptiveMutex(final boolean useAdaptiveMutex) {
        dbOptions.setUseAdaptiveMutex(useAdaptiveMutex);
        return this;
    }

    @Override
    public long bytesPerSync() {
        return dbOptions.bytesPerSync();
    }

    @Override
    public Options setBytesPerSync(final long bytesPerSync) {
        dbOptions.setBytesPerSync(bytesPerSync);
        return this;
    }

    @Override
    public Options setWalBytesPerSync(final long walBytesPerSync) {
        logIgnoreWalOption("walBytesPerSync");
        return this;
    }

    @Override
    public long walBytesPerSync() {
        return dbOptions.walBytesPerSync();
    }

    @Override
    public Options setEnableThreadTracking(final boolean enableThreadTracking) {
        dbOptions.setEnableThreadTracking(enableThreadTracking);
        return this;
    }

    @Override
    public boolean enableThreadTracking() {
        return dbOptions.enableThreadTracking();
    }

    @Override
    public Options setDelayedWriteRate(final long delayedWriteRate) {
        dbOptions.setDelayedWriteRate(delayedWriteRate);
        return this;
    }

    @Override
    public long delayedWriteRate() {
        return dbOptions.delayedWriteRate();
    }

    @Override
    public Options setAllowConcurrentMemtableWrite(final boolean allowConcurrentMemtableWrite) {
        dbOptions.setAllowConcurrentMemtableWrite(allowConcurrentMemtableWrite);
        return this;
    }

    @Override
    public boolean allowConcurrentMemtableWrite() {
        return dbOptions.allowConcurrentMemtableWrite();
    }

    @Override
    public Options setEnableWriteThreadAdaptiveYield(final boolean enableWriteThreadAdaptiveYield) {
        dbOptions.setEnableWriteThreadAdaptiveYield(enableWriteThreadAdaptiveYield);
        return this;
    }

    @Override
    public boolean enableWriteThreadAdaptiveYield() {
        return dbOptions.enableWriteThreadAdaptiveYield();
    }

    @Override
    public Options setWriteThreadMaxYieldUsec(final long writeThreadMaxYieldUsec) {
        dbOptions.setWriteThreadMaxYieldUsec(writeThreadMaxYieldUsec);
        return this;
    }

    @Override
    public long writeThreadMaxYieldUsec() {
        return dbOptions.writeThreadMaxYieldUsec();
    }

    @Override
    public Options setWriteThreadSlowYieldUsec(final long writeThreadSlowYieldUsec) {
        dbOptions.setWriteThreadSlowYieldUsec(writeThreadSlowYieldUsec);
        return this;
    }

    @Override
    public long writeThreadSlowYieldUsec() {
        return dbOptions.writeThreadSlowYieldUsec();
    }

    @Override
    public Options setSkipStatsUpdateOnDbOpen(final boolean skipStatsUpdateOnDbOpen) {
        dbOptions.setSkipStatsUpdateOnDbOpen(skipStatsUpdateOnDbOpen);
        return this;
    }

    @Override
    public boolean skipStatsUpdateOnDbOpen() {
        return dbOptions.skipStatsUpdateOnDbOpen();
    }

    @Override
    public Options setWalRecoveryMode(final WALRecoveryMode walRecoveryMode) {
        logIgnoreWalOption("walRecoveryMode");
        return this;
    }

    @Override
    public WALRecoveryMode walRecoveryMode() {
        return dbOptions.walRecoveryMode();
    }

    @Override
    public Options setAllow2pc(final boolean allow2pc) {
        dbOptions.setAllow2pc(allow2pc);
        return this;
    }

    @Override
    public boolean allow2pc() {
        return dbOptions.allow2pc();
    }

    @Override
    public Options setRowCache(final Cache rowCache) {
        dbOptions.setRowCache(rowCache);
        return this;
    }

    @Override
    public Cache rowCache() {
        return dbOptions.rowCache();
    }

    @Override
    public Options setFailIfOptionsFileError(final boolean failIfOptionsFileError) {
        dbOptions.setFailIfOptionsFileError(failIfOptionsFileError);
        return this;
    }

    @Override
    public boolean failIfOptionsFileError() {
        return dbOptions.failIfOptionsFileError();
    }

    @Override
    public Options setDumpMallocStats(final boolean dumpMallocStats) {
        dbOptions.setDumpMallocStats(dumpMallocStats);
        return this;
    }

    @Override
    public boolean dumpMallocStats() {
        return dbOptions.dumpMallocStats();
    }

    @Override
    public Options setAvoidFlushDuringRecovery(final boolean avoidFlushDuringRecovery) {
        dbOptions.setAvoidFlushDuringRecovery(avoidFlushDuringRecovery);
        return this;
    }

    @Override
    public boolean avoidFlushDuringRecovery() {
        return dbOptions.avoidFlushDuringRecovery();
    }

    @Override
    public Options setAvoidFlushDuringShutdown(final boolean avoidFlushDuringShutdown) {
        dbOptions.setAvoidFlushDuringShutdown(avoidFlushDuringShutdown);
        return this;
    }

    @Override
    public boolean avoidFlushDuringShutdown() {
        return dbOptions.avoidFlushDuringShutdown();
    }

    @Override
    public MemTableConfig memTableConfig() {
        return columnFamilyOptions.memTableConfig();
    }

    @Override
    public Options setMemTableConfig(final MemTableConfig config) {
        columnFamilyOptions.setMemTableConfig(config);
        return this;
    }

    @Deprecated(since = "4.2", forRemoval = true)
    @Override
    public Options setRateLimiter(final RateLimiter rateLimiter) {
        log.warn("rate_limiter has been deprecated in RocksDB v7.6.0." +
                " See https://github.com/facebook/rocksdb/pull/10378");
        dbOptions.setRateLimiter(rateLimiter);
        return this;
    }

    @Override
    public Options setSstFileManager(final SstFileManager sstFileManager) {
        dbOptions.setSstFileManager(sstFileManager);
        return this;
    }

    @Override
    public Options setLogger(final LoggerInterface logger) {
        dbOptions.setLogger(logger);
        return this;
    }

    @Override
    public Options setInfoLogLevel(final InfoLogLevel infoLogLevel) {
        dbOptions.setInfoLogLevel(infoLogLevel);
        return this;
    }

    @Override
    public InfoLogLevel infoLogLevel() {
        return dbOptions.infoLogLevel();
    }

    @Override
    public String memTableFactoryName() {
        return columnFamilyOptions.memTableFactoryName();
    }

    @Override
    public TableFormatConfig tableFormatConfig() {
        return columnFamilyOptions.tableFormatConfig();
    }

    @Override
    public Options setTableFormatConfig(final TableFormatConfig config) {
        columnFamilyOptions.setTableFormatConfig(config);
        return this;
    }

    @Override
    public String tableFactoryName() {
        return columnFamilyOptions.tableFactoryName();
    }

    @Override
    public Options useFixedLengthPrefixExtractor(final int n) {
        columnFamilyOptions.useFixedLengthPrefixExtractor(n);
        return this;
    }

    @Override
    public Options useCappedPrefixExtractor(final int n) {
        columnFamilyOptions.useCappedPrefixExtractor(n);
        return this;
    }

    @Override
    public CompressionType compressionType() {
        return columnFamilyOptions.compressionType();
    }

    @Override
    public Options setCompressionPerLevel(final List<CompressionType> compressionLevels) {
        columnFamilyOptions.setCompressionPerLevel(compressionLevels);
        return this;
    }

    @Override
    public List<CompressionType> compressionPerLevel() {
        return columnFamilyOptions.compressionPerLevel();
    }

    @Override
    public Options setCompressionType(final CompressionType compressionType) {
        columnFamilyOptions.setCompressionType(compressionType);
        return this;
    }

    @Override
    public Options setMemtableMaxRangeDeletions(final int n) {
        columnFamilyOptions.setMemtableMaxRangeDeletions(n);
        return this;
    }

    @Override
    public int memtableMaxRangeDeletions() {
        return columnFamilyOptions.memtableMaxRangeDeletions();
    }

    @Override
    public Options setBottommostCompressionType(final CompressionType bottommostCompressionType) {
        columnFamilyOptions.setBottommostCompressionType(bottommostCompressionType);
        return this;
    }

    @Override
    public CompressionType bottommostCompressionType() {
        return columnFamilyOptions.bottommostCompressionType();
    }

    @Override
    public Options setCompressionOptions(final CompressionOptions compressionOptions) {
        columnFamilyOptions.setCompressionOptions(compressionOptions);
        return this;
    }

    @Override
    public CompressionOptions compressionOptions() {
        return columnFamilyOptions.compressionOptions();
    }

    @Override
    public CompactionStyle compactionStyle() {
        return columnFamilyOptions.compactionStyle();
    }

    @Override
    public Options setCompactionStyle(final CompactionStyle compactionStyle) {
        columnFamilyOptions.setCompactionStyle(compactionStyle);
        return this;
    }

    @Override
    public int numLevels() {
        return columnFamilyOptions.numLevels();
    }

    @Override
    public Options setNumLevels(final int numLevels) {
        columnFamilyOptions.setNumLevels(numLevels);
        return this;
    }

    @Override
    public int levelZeroFileNumCompactionTrigger() {
        return columnFamilyOptions.levelZeroFileNumCompactionTrigger();
    }

    @Override
    public Options setLevelZeroFileNumCompactionTrigger(final int numFiles) {
        columnFamilyOptions.setLevelZeroFileNumCompactionTrigger(numFiles);
        return this;
    }

    @Override
    public int levelZeroSlowdownWritesTrigger() {
        return columnFamilyOptions.levelZeroSlowdownWritesTrigger();
    }

    @Override
    public Options setLevelZeroSlowdownWritesTrigger(final int numFiles) {
        columnFamilyOptions.setLevelZeroSlowdownWritesTrigger(numFiles);
        return this;
    }

    @Override
    public int levelZeroStopWritesTrigger() {
        return columnFamilyOptions.levelZeroStopWritesTrigger();
    }

    @Override
    public Options setLevelZeroStopWritesTrigger(final int numFiles) {
        columnFamilyOptions.setLevelZeroStopWritesTrigger(numFiles);
        return this;
    }

    @Override
    public long targetFileSizeBase() {
        return columnFamilyOptions.targetFileSizeBase();
    }

    @Override
    public Options setTargetFileSizeBase(final long targetFileSizeBase) {
        columnFamilyOptions.setTargetFileSizeBase(targetFileSizeBase);
        return this;
    }

    @Override
    public int targetFileSizeMultiplier() {
        return columnFamilyOptions.targetFileSizeMultiplier();
    }

    @Override
    public Options setTargetFileSizeMultiplier(final int multiplier) {
        columnFamilyOptions.setTargetFileSizeMultiplier(multiplier);
        return this;
    }

    @Override
    public Options setMaxBytesForLevelBase(final long maxBytesForLevelBase) {
        columnFamilyOptions.setMaxBytesForLevelBase(maxBytesForLevelBase);
        return this;
    }

    @Override
    public long maxBytesForLevelBase() {
        return columnFamilyOptions.maxBytesForLevelBase();
    }

    @Override
    public Options setLevelCompactionDynamicLevelBytes(final boolean enableLevelCompactionDynamicLevelBytes) {
        columnFamilyOptions.setLevelCompactionDynamicLevelBytes(enableLevelCompactionDynamicLevelBytes);
        return this;
    }

    @Override
    public boolean levelCompactionDynamicLevelBytes() {
        return columnFamilyOptions.levelCompactionDynamicLevelBytes();
    }

    @Override
    public double maxBytesForLevelMultiplier() {
        return columnFamilyOptions.maxBytesForLevelMultiplier();
    }

    @Override
    public Options setMaxBytesForLevelMultiplier(final double multiplier) {
        columnFamilyOptions.setMaxBytesForLevelMultiplier(multiplier);
        return this;
    }

    @Override
    public long maxCompactionBytes() {
        return columnFamilyOptions.maxCompactionBytes();
    }

    @Override
    public Options setMaxCompactionBytes(final long maxCompactionBytes) {
        columnFamilyOptions.setMaxCompactionBytes(maxCompactionBytes);
        return this;
    }

    @Override
    public long arenaBlockSize() {
        return columnFamilyOptions.arenaBlockSize();
    }

    @Override
    public Options setArenaBlockSize(final long arenaBlockSize) {
        columnFamilyOptions.setArenaBlockSize(arenaBlockSize);
        return this;
    }

    @Override
    public boolean disableAutoCompactions() {
        return columnFamilyOptions.disableAutoCompactions();
    }

    @Override
    public Options setDisableAutoCompactions(final boolean disableAutoCompactions) {
        columnFamilyOptions.setDisableAutoCompactions(disableAutoCompactions);
        return this;
    }

    @Override
    public long maxSequentialSkipInIterations() {
        return columnFamilyOptions.maxSequentialSkipInIterations();
    }

    @Override
    public Options setMaxSequentialSkipInIterations(final long maxSequentialSkipInIterations) {
        columnFamilyOptions.setMaxSequentialSkipInIterations(maxSequentialSkipInIterations);
        return this;
    }

    @Override
    public boolean inplaceUpdateSupport() {
        return columnFamilyOptions.inplaceUpdateSupport();
    }

    @Override
    public Options setInplaceUpdateSupport(final boolean inplaceUpdateSupport) {
        columnFamilyOptions.setInplaceUpdateSupport(inplaceUpdateSupport);
        return this;
    }

    @Override
    public long inplaceUpdateNumLocks() {
        return columnFamilyOptions.inplaceUpdateNumLocks();
    }

    @Override
    public Options setInplaceUpdateNumLocks(final long inplaceUpdateNumLocks) {
        columnFamilyOptions.setInplaceUpdateNumLocks(inplaceUpdateNumLocks);
        return this;
    }

    @Override
    public double memtablePrefixBloomSizeRatio() {
        return columnFamilyOptions.memtablePrefixBloomSizeRatio();
    }

    @Override
    public Options setMemtablePrefixBloomSizeRatio(final double memtablePrefixBloomSizeRatio) {
        columnFamilyOptions.setMemtablePrefixBloomSizeRatio(memtablePrefixBloomSizeRatio);
        return this;
    }

    @Override
    public int bloomLocality() {
        return columnFamilyOptions.bloomLocality();
    }

    @Override
    public Options setBloomLocality(final int bloomLocality) {
        columnFamilyOptions.setBloomLocality(bloomLocality);
        return this;
    }

    @Override
    public long maxSuccessiveMerges() {
        return columnFamilyOptions.maxSuccessiveMerges();
    }

    @Override
    public Options setMaxSuccessiveMerges(final long maxSuccessiveMerges) {
        columnFamilyOptions.setMaxSuccessiveMerges(maxSuccessiveMerges);
        return this;
    }

    @Override
    public int minWriteBufferNumberToMerge() {
        return columnFamilyOptions.minWriteBufferNumberToMerge();
    }

    @Override
    public Options setMinWriteBufferNumberToMerge(final int minWriteBufferNumberToMerge) {
        columnFamilyOptions.setMinWriteBufferNumberToMerge(minWriteBufferNumberToMerge);
        return this;
    }

    @Override
    public Options setOptimizeFiltersForHits(final boolean optimizeFiltersForHits) {
        columnFamilyOptions.setOptimizeFiltersForHits(optimizeFiltersForHits);
        return this;
    }

    @Override
    public boolean optimizeFiltersForHits() {
        return columnFamilyOptions.optimizeFiltersForHits();
    }

    @Override
    public Options setMemtableHugePageSize(final long memtableHugePageSize) {
        columnFamilyOptions.setMemtableHugePageSize(memtableHugePageSize);
        return this;
    }

    @Override
    public long memtableHugePageSize() {
        return columnFamilyOptions.memtableHugePageSize();
    }

    @Override
    public Options setSoftPendingCompactionBytesLimit(final long softPendingCompactionBytesLimit) {
        columnFamilyOptions.setSoftPendingCompactionBytesLimit(softPendingCompactionBytesLimit);
        return this;
    }

    @Override
    public long softPendingCompactionBytesLimit() {
        return columnFamilyOptions.softPendingCompactionBytesLimit();
    }

    @Override
    public Options setHardPendingCompactionBytesLimit(final long hardPendingCompactionBytesLimit) {
        columnFamilyOptions.setHardPendingCompactionBytesLimit(hardPendingCompactionBytesLimit);
        return this;
    }

    @Override
    public long hardPendingCompactionBytesLimit() {
        return columnFamilyOptions.hardPendingCompactionBytesLimit();
    }

    @Override
    public Options setLevel0FileNumCompactionTrigger(final int level0FileNumCompactionTrigger) {
        columnFamilyOptions.setLevel0FileNumCompactionTrigger(level0FileNumCompactionTrigger);
        return this;
    }

    @Override
    public int level0FileNumCompactionTrigger() {
        return columnFamilyOptions.level0FileNumCompactionTrigger();
    }

    @Override
    public Options setLevel0SlowdownWritesTrigger(final int level0SlowdownWritesTrigger) {
        columnFamilyOptions.setLevel0SlowdownWritesTrigger(level0SlowdownWritesTrigger);
        return this;
    }

    @Override
    public int level0SlowdownWritesTrigger() {
        return columnFamilyOptions.level0SlowdownWritesTrigger();
    }

    @Override
    public Options setLevel0StopWritesTrigger(final int level0StopWritesTrigger) {
        columnFamilyOptions.setLevel0StopWritesTrigger(level0StopWritesTrigger);
        return this;
    }

    @Override
    public int level0StopWritesTrigger() {
        return columnFamilyOptions.level0StopWritesTrigger();
    }

    @Override
    public Options setMaxBytesForLevelMultiplierAdditional(final int[] maxBytesForLevelMultiplierAdditional) {
        columnFamilyOptions.setMaxBytesForLevelMultiplierAdditional(maxBytesForLevelMultiplierAdditional);
        return this;
    }

    @Override
    public int[] maxBytesForLevelMultiplierAdditional() {
        return columnFamilyOptions.maxBytesForLevelMultiplierAdditional();
    }

    @Override
    public Options setParanoidFileChecks(final boolean paranoidFileChecks) {
        columnFamilyOptions.setParanoidFileChecks(paranoidFileChecks);
        return this;
    }

    @Override
    public boolean paranoidFileChecks() {
        return columnFamilyOptions.paranoidFileChecks();
    }

    @Override
    public Options setMaxWriteBufferNumberToMaintain(final int maxWriteBufferNumberToMaintain) {
        columnFamilyOptions.setMaxWriteBufferNumberToMaintain(maxWriteBufferNumberToMaintain);
        return this;
    }

    @Override
    public int maxWriteBufferNumberToMaintain() {
        return columnFamilyOptions.maxWriteBufferNumberToMaintain();
    }

    @Override
    public Options setCompactionPriority(final CompactionPriority compactionPriority) {
        columnFamilyOptions.setCompactionPriority(compactionPriority);
        return this;
    }

    @Override
    public CompactionPriority compactionPriority() {
        return columnFamilyOptions.compactionPriority();
    }

    @Override
    public Options setReportBgIoStats(final boolean reportBgIoStats) {
        columnFamilyOptions.setReportBgIoStats(reportBgIoStats);
        return this;
    }

    @Override
    public boolean reportBgIoStats() {
        return columnFamilyOptions.reportBgIoStats();
    }

    @Override
    public Options setCompactionOptionsUniversal(final CompactionOptionsUniversal compactionOptionsUniversal) {
        columnFamilyOptions.setCompactionOptionsUniversal(compactionOptionsUniversal);
        return this;
    }

    @Override
    public CompactionOptionsUniversal compactionOptionsUniversal() {
        return columnFamilyOptions.compactionOptionsUniversal();
    }

    @Override
    public Options setCompactionOptionsFIFO(final CompactionOptionsFIFO compactionOptionsFIFO) {
        columnFamilyOptions.setCompactionOptionsFIFO(compactionOptionsFIFO);
        return this;
    }

    @Override
    public CompactionOptionsFIFO compactionOptionsFIFO() {
        return columnFamilyOptions.compactionOptionsFIFO();
    }

    @Override
    public Options setForceConsistencyChecks(final boolean forceConsistencyChecks) {
        columnFamilyOptions.setForceConsistencyChecks(forceConsistencyChecks);
        return this;
    }

    @Override
    public boolean forceConsistencyChecks() {
        return columnFamilyOptions.forceConsistencyChecks();
    }

    @Override
    public Options setWriteBufferManager(final WriteBufferManager writeBufferManager) {
        dbOptions.setWriteBufferManager(writeBufferManager);
        return this;
    }

    @Override
    public WriteBufferManager writeBufferManager() {
        return dbOptions.writeBufferManager();
    }

    @Override
    public Options setMaxWriteBatchGroupSizeBytes(final long maxWriteBatchGroupSizeBytes) {
        dbOptions.setMaxWriteBatchGroupSizeBytes(maxWriteBatchGroupSizeBytes);
        return this;
    }

    @Override
    public long maxWriteBatchGroupSizeBytes() {
        return dbOptions.maxWriteBatchGroupSizeBytes();
    }

    @Override
    public Options oldDefaults(final int majorVersion, final int minorVersion) {
        columnFamilyOptions.oldDefaults(majorVersion, minorVersion);
        return this;
    }

    @Override
    public Options optimizeForSmallDb(final Cache cache) {
        return super.optimizeForSmallDb(cache);
    }

    @Override
    public AbstractCompactionFilter<? extends AbstractSlice<?>> compactionFilter() {
        return columnFamilyOptions.compactionFilter();
    }

    @Override
    public AbstractCompactionFilterFactory<? extends AbstractCompactionFilter<?>> compactionFilterFactory() {
        return columnFamilyOptions.compactionFilterFactory();
    }

    @Override
    public Options setStatsPersistPeriodSec(final int statsPersistPeriodSec) {
        dbOptions.setStatsPersistPeriodSec(statsPersistPeriodSec);
        return this;
    }

    @Override
    public int statsPersistPeriodSec() {
        return dbOptions.statsPersistPeriodSec();
    }

    @Override
    public Options setStatsHistoryBufferSize(final long statsHistoryBufferSize) {
        dbOptions.setStatsHistoryBufferSize(statsHistoryBufferSize);
        return this;
    }

    @Override
    public long statsHistoryBufferSize() {
        return dbOptions.statsHistoryBufferSize();
    }

    @Override
    public Options setStrictBytesPerSync(final boolean strictBytesPerSync) {
        dbOptions.setStrictBytesPerSync(strictBytesPerSync);
        return this;
    }

    @Override
    public boolean strictBytesPerSync() {
        return dbOptions.strictBytesPerSync();
    }

    @Override
    public Options setListeners(final List<AbstractEventListener> listeners) {
        dbOptions.setListeners(listeners);
        return this;
    }

    @Override
    public List<AbstractEventListener> listeners() {
        return dbOptions.listeners();
    }

    @Override
    public Options setEnablePipelinedWrite(final boolean enablePipelinedWrite) {
        dbOptions.setEnablePipelinedWrite(enablePipelinedWrite);
        return this;
    }

    @Override
    public boolean enablePipelinedWrite() {
        return dbOptions.enablePipelinedWrite();
    }

    @Override
    public Options setUnorderedWrite(final boolean unorderedWrite) {
        dbOptions.setUnorderedWrite(unorderedWrite);
        return this;
    }

    @Override
    public boolean unorderedWrite() {
        return dbOptions.unorderedWrite();
    }

    @Override
    public Options setSkipCheckingSstFileSizesOnDbOpen(final boolean skipCheckingSstFileSizesOnDbOpen) {
        dbOptions.setSkipCheckingSstFileSizesOnDbOpen(skipCheckingSstFileSizesOnDbOpen);
        return this;
    }

    @Override
    public boolean skipCheckingSstFileSizesOnDbOpen() {
        return dbOptions.skipCheckingSstFileSizesOnDbOpen();
    }

    @Override
    public Options setWalFilter(final AbstractWalFilter walFilter) {
        logIgnoreWalOption("walFilter");
        return this;
    }

    @Override
    public WalFilter walFilter() {
        return dbOptions.walFilter();
    }

    @Override
    public Options setAllowIngestBehind(final boolean allowIngestBehind) {
        dbOptions.setAllowIngestBehind(allowIngestBehind);
        return this;
    }

    @Override
    public boolean allowIngestBehind() {
        return dbOptions.allowIngestBehind();
    }

    @Override
    public Options setTwoWriteQueues(final boolean twoWriteQueues) {
        dbOptions.setTwoWriteQueues(twoWriteQueues);
        return this;
    }

    @Override
    public boolean twoWriteQueues() {
        return dbOptions.twoWriteQueues();
    }

    @Override
    public Options setManualWalFlush(final boolean manualWalFlush) {
        logIgnoreWalOption("manualWalFlush");
        return this;
    }

    @Override
    public boolean manualWalFlush() {
        return dbOptions.manualWalFlush();
    }

    @Override
    public Options setCfPaths(final Collection<DbPath> cfPaths) {
        columnFamilyOptions.setCfPaths(cfPaths);
        return this;
    }

    @Override
    public List<DbPath> cfPaths() {
        return columnFamilyOptions.cfPaths();
    }

    @Override
    public Options setBottommostCompressionOptions(final CompressionOptions bottommostCompressionOptions) {
        columnFamilyOptions.setBottommostCompressionOptions(bottommostCompressionOptions);
        return this;
    }

    @Override
    public CompressionOptions bottommostCompressionOptions() {
        return columnFamilyOptions.bottommostCompressionOptions();
    }

    @Override
    public Options setTtl(final long ttl) {
        columnFamilyOptions.setTtl(ttl);
        return this;
    }

    @Override
    public long ttl() {
        return columnFamilyOptions.ttl();
    }

    @Override
    public Options setPeriodicCompactionSeconds(final long periodicCompactionSeconds) {
        columnFamilyOptions.setPeriodicCompactionSeconds(periodicCompactionSeconds);
        return this;
    }

    @Override
    public long periodicCompactionSeconds() {
        return columnFamilyOptions.periodicCompactionSeconds();
    }

    @Override
    public Options setAtomicFlush(final boolean atomicFlush) {
        dbOptions.setAtomicFlush(atomicFlush);
        return this;
    }

    @Override
    public boolean atomicFlush() {
        return dbOptions.atomicFlush();
    }

    @Override
    public Options setAvoidUnnecessaryBlockingIO(final boolean avoidUnnecessaryBlockingIO) {
        dbOptions.setAvoidUnnecessaryBlockingIO(avoidUnnecessaryBlockingIO);
        return this;
    }

    @Override
    public boolean avoidUnnecessaryBlockingIO() {
        return dbOptions.avoidUnnecessaryBlockingIO();
    }

    @Override
    public Options setPersistStatsToDisk(final boolean persistStatsToDisk) {
        dbOptions.setPersistStatsToDisk(persistStatsToDisk);
        return this;
    }

    @Override
    public boolean persistStatsToDisk() {
        return dbOptions.persistStatsToDisk();
    }

    @Override
    public Options setWriteDbidToManifest(final boolean writeDbidToManifest) {
        dbOptions.setWriteDbidToManifest(writeDbidToManifest);
        return this;
    }

    @Override
    public boolean writeDbidToManifest() {
        return dbOptions.writeDbidToManifest();
    }

    @Override
    public Options setLogReadaheadSize(final long logReadaheadSize) {
        dbOptions.setLogReadaheadSize(logReadaheadSize);
        return this;
    }

    @Override
    public long logReadaheadSize() {
        return dbOptions.logReadaheadSize();
    }

    @Override
    public Options setBestEffortsRecovery(final boolean bestEffortsRecovery) {
        dbOptions.setBestEffortsRecovery(bestEffortsRecovery);
        return this;
    }

    @Override
    public boolean bestEffortsRecovery() {
        return dbOptions.bestEffortsRecovery();
    }

    @Override
    public Options setMaxBgErrorResumeCount(final int maxBgerrorResumeCount) {
        dbOptions.setMaxBgErrorResumeCount(maxBgerrorResumeCount);
        return this;
    }

    @Override
    public int maxBgerrorResumeCount() {
        return dbOptions.maxBgerrorResumeCount();
    }

    @Override
    public Options setBgerrorResumeRetryInterval(final long bgerrorResumeRetryInterval) {
        dbOptions.setBgerrorResumeRetryInterval(bgerrorResumeRetryInterval);
        return this;
    }

    @Override
    public long bgerrorResumeRetryInterval() {
        return dbOptions.bgerrorResumeRetryInterval();
    }

    @Override
    public Options setSstPartitionerFactory(final SstPartitionerFactory sstPartitionerFactory) {
        columnFamilyOptions.setSstPartitionerFactory(sstPartitionerFactory);
        return this;
    }

    @Override
    public SstPartitionerFactory sstPartitionerFactory() {
        return columnFamilyOptions.sstPartitionerFactory();
    }

    @Override
    public Options setCompactionThreadLimiter(final ConcurrentTaskLimiter compactionThreadLimiter) {
        columnFamilyOptions.setCompactionThreadLimiter(compactionThreadLimiter);
        return this;
    }

    @Override
    public ConcurrentTaskLimiter compactionThreadLimiter() {
        return columnFamilyOptions.compactionThreadLimiter();
    }

    @Override
    public Options setCompactionFilter(final AbstractCompactionFilter<? extends AbstractSlice<?>> compactionFilter) {
        columnFamilyOptions.setCompactionFilter(compactionFilter);
        return this;
    }

    @Override
    public Options setCompactionFilterFactory(final AbstractCompactionFilterFactory<? extends AbstractCompactionFilter<?>> compactionFilterFactory) {
        columnFamilyOptions.setCompactionFilterFactory(compactionFilterFactory);
        return this;
    }

    @Override
    public Options setBlobCompactionReadaheadSize(final long blobCompactionReadaheadSize) {
        columnFamilyOptions.setBlobCompactionReadaheadSize(blobCompactionReadaheadSize);
        return this;
    }

    @Override
    public long blobCompactionReadaheadSize() {
        return columnFamilyOptions.blobCompactionReadaheadSize();
    }

    @Override
    public Options setMemtableWholeKeyFiltering(final boolean memtableWholeKeyFiltering) {
        columnFamilyOptions.setMemtableWholeKeyFiltering(memtableWholeKeyFiltering);
        return this;
    }

    @Override
    public boolean memtableWholeKeyFiltering() {
        return columnFamilyOptions.memtableWholeKeyFiltering();
    }
    
    @Override
    public Options setExperimentalMempurgeThreshold(final double experimentalMempurgeThreshold) {
        columnFamilyOptions.setExperimentalMempurgeThreshold(experimentalMempurgeThreshold);
        return this;
    }

    @Override
    public double experimentalMempurgeThreshold() {
        return columnFamilyOptions.experimentalMempurgeThreshold();
    }

    //
    // BEGIN options for blobs (integrated BlobDB)
    //
    
    @Override
    public Options setEnableBlobFiles(final boolean enableBlobFiles) {
        columnFamilyOptions.setEnableBlobFiles(enableBlobFiles);
        return this;
    }

    @Override
    public boolean enableBlobFiles() {
        return columnFamilyOptions.enableBlobFiles();
    }

    @Override
    public Options setMinBlobSize(final long minBlobSize) {
        columnFamilyOptions.setMinBlobSize(minBlobSize);
        return this;
    }

    @Override
    public long minBlobSize() {
        return columnFamilyOptions.minBlobSize();
    }

    @Override
    public Options setBlobFileSize(final long blobFileSize) {
        columnFamilyOptions.setBlobFileSize(blobFileSize);
        return this;
    }

    @Override
    public long blobFileSize() {
        return columnFamilyOptions.blobFileSize();
    }

    @Override
    public Options setBlobCompressionType(final CompressionType compressionType) {
        columnFamilyOptions.setBlobCompressionType(compressionType);
        return this;
    }

    @Override
    public CompressionType blobCompressionType() {
        return columnFamilyOptions.blobCompressionType();
    }

    @Override
    public Options setEnableBlobGarbageCollection(final boolean enableBlobGarbageCollection) {
        columnFamilyOptions.setEnableBlobGarbageCollection(enableBlobGarbageCollection);
        return this;
    }

    @Override
    public boolean enableBlobGarbageCollection() {
        return columnFamilyOptions.enableBlobGarbageCollection();
    }

    @Override
    public Options setBlobGarbageCollectionAgeCutoff(final double blobGarbageCollectionAgeCutoff) {
        columnFamilyOptions.setBlobGarbageCollectionAgeCutoff(blobGarbageCollectionAgeCutoff);
        return this;
    }

    @Override
    public double blobGarbageCollectionAgeCutoff() {
        return columnFamilyOptions.blobGarbageCollectionAgeCutoff();
    }

    @Override
    public Options setBlobGarbageCollectionForceThreshold(final double blobGarbageCollectionForceThreshold) {
        columnFamilyOptions.setBlobGarbageCollectionForceThreshold(blobGarbageCollectionForceThreshold);
        return this;
    }

    @Override
    public double blobGarbageCollectionForceThreshold() {
        return columnFamilyOptions.blobGarbageCollectionForceThreshold();
    }


    @Override
    public Options setPrepopulateBlobCache(final PrepopulateBlobCache prepopulateBlobCache) {
        columnFamilyOptions.setPrepopulateBlobCache(prepopulateBlobCache);
        return this;
    }

    @Override
    public PrepopulateBlobCache prepopulateBlobCache() {
        return columnFamilyOptions.prepopulateBlobCache();
    }

    @Override
    public Options setBlobFileStartingLevel(final int blobFileStartingLevel) {
        columnFamilyOptions.setBlobFileStartingLevel(blobFileStartingLevel);
        return this;
    }

    @Override
    public int blobFileStartingLevel() {
        return columnFamilyOptions.blobFileStartingLevel();
    }

    @Override
    public Options setDailyOffpeakTimeUTC(final String offpeakTimeUTC) {
        dbOptions.setDailyOffpeakTimeUTC(offpeakTimeUTC);
        return this;
    }

    @Override
    public String dailyOffpeakTimeUTC() {
        return dbOptions.dailyOffpeakTimeUTC();
    }

    //
    // END options for blobs (integrated BlobDB)
    //

    @Override
    public void close() {
        // ColumnFamilyOptions should be closed after DBOptions
        dbOptions.close();
        columnFamilyOptions.close();
        // close super last since we initialized it first
        super.close();
    }

    private void logIgnoreWalOption(final String option) {
        log.warn("WAL is explicitly disabled by Streams in RocksDB. Setting option '{}' will be ignored", option);
    }
}
