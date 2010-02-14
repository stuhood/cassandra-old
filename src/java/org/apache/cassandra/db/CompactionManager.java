/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db;

import java.io.IOException;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import javax.management.*;

import org.apache.log4j.Logger;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.AntiEntropyService;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import java.net.InetAddress;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.collections.iterators.FilterIterator;
import org.apache.commons.collections.iterators.CollatingIterator;
import org.apache.commons.collections.PredicateUtils;

public class CompactionManager implements CompactionManagerMBean
{
    public static final String MBEAN_OBJECT_NAME = "org.apache.cassandra.db:type=CompactionManager";
    private static final Logger logger = Logger.getLogger(CompactionManager.class);
    public static final CompactionManager instance;

    // approximate minimum bucket size in bytes
    private final static long MIN_BUCKET_SIZE = 50 * 1024 * 1024;

    // compact this many sstables max at a time (mutable for testing only)
    private int maximumCompactionThreshold = 256;

    static
    {
        instance = new CompactionManager();
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(instance, new ObjectName(MBEAN_OBJECT_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private CompactionExecutor executor = new CompactionExecutor();
    private Map<ColumnFamilyStore, Integer> estimatedCompactions = new NonBlockingHashMap<ColumnFamilyStore, Integer>();

    private int getMergeFactor(ColumnFamilyStore cfs)
    {
        return Math.min(maximumCompactionThreshold-1,
                        DatabaseDescriptor.getMergeFactor(cfs.getTableName(),
                                                          cfs.getColumnFamilyName()));
    }

    /**
     * Call this whenever a compaction might be needed on the given columnfamily.
     * It's okay to over-call (within reason) since the compactions are single-threaded,
     * and if a call is unnecessary, it will just be no-oped in the bucketing phase.
     */
    public Future<Integer> submitMinorIfNeeded(final ColumnFamilyStore cfs)
    {
        Callable<Integer> callable = new Callable<Integer>()
        {
            public Integer call() throws IOException
            {
                if (maximumCompactionThreshold <= 0)
                {
                    logger.debug("Compaction is currently disabled.");
                    return 0;
                }

                int mergeFactor = getMergeFactor(cfs);
                logger.debug("Checking to see if compaction of " + cfs.columnFamily_ + " would be useful (mergeFactor: " + mergeFactor + ")");
                NavigableMap<Integer, List<SSTableReader>> buckets = getCompactionBuckets(cfs.getSSTables(), mergeFactor);
                updateEstimateFor(cfs, buckets.values(), mergeFactor);
                
                for (List<SSTableReader> sstables : buckets.values())
                {
                    if (sstables.size() >= mergeFactor)
                    {
                        // if we have too many to compact all at once, compact older ones first -- this avoids
                        // re-compacting files we just created.
                        Collections.sort(sstables);
                        return doCompaction(cfs, sstables.subList(0, Math.min(sstables.size(), maximumCompactionThreshold)), getDefaultGCBefore());
                    }
                }
                return 0;
            }
        };
        return executor.submit(callable);
    }

    private void updateEstimateFor(ColumnFamilyStore cfs, Collection<List<SSTableReader>> buckets, int mergeFactor)
    {
        int n = 0;
        for (List<SSTableReader> sstables : buckets)
        {
            if (sstables.size() >= mergeFactor)
            {
                n += 1 + sstables.size() / (maximumCompactionThreshold - mergeFactor);
            }
        }
        estimatedCompactions.put(cfs, n);
    }

    public Future<Object> submitCleanup(final ColumnFamilyStore cfStore)
    {
        Callable<Object> runnable = new Callable<Object>()
        {
            public Object call() throws IOException
            {
                doCleanupCompaction(cfStore);
                return this;
            }
        };
        return executor.submit(runnable);
    }

    public Future<List<SSTableReader>> submitAnticompaction(final ColumnFamilyStore cfStore, final Collection<Range> ranges, final InetAddress target)
    {
        Callable<List<SSTableReader>> callable = new Callable<List<SSTableReader>>()
        {
            public List<SSTableReader> call() throws IOException
            {
                return doAntiCompaction(cfStore, cfStore.getSSTables(), ranges, target);
            }
        };
        return executor.submit(callable);
    }

    public Future submitMajor(final ColumnFamilyStore cfStore)
    {
        return submitMajor(cfStore, 0, getDefaultGCBefore());
    }

    public Future submitMajor(final ColumnFamilyStore cfStore, final long skip, final int gcBefore)
    {
        Callable<Object> callable = new Callable<Object>()
        {
            public Object call() throws IOException
            {
                Collection<SSTableReader> sstables;
                if (skip > 0)
                {
                    sstables = new ArrayList<SSTableReader>();
                    for (SSTableReader sstable : cfStore.getSSTables())
                    {
                        if (sstable.length() < skip * 1024L * 1024L * 1024L)
                        {
                            sstables.add(sstable);
                        }
                    }
                }
                else
                {
                    sstables = cfStore.getSSTables();
                }

                doCompaction(cfStore, sstables, gcBefore);
                return this;
            }
        };
        return executor.submit(callable);
    }

    public Future submitReadonly(final ColumnFamilyStore cfStore, final InetAddress initiator)
    {
        Callable<Object> callable = new Callable<Object>()
        {
            public Object call() throws IOException
            {
                doReadonlyCompaction(cfStore, initiator);
                return this;
            }
        };
        return executor.submit(callable);
    }

    /**
     * For testing only.
     */
    void disableAutoCompaction()
    {
        maximumCompactionThreshold = 0;
    }

    /**
     * For internal use and testing only.  The rest of the system should go through the submit* methods,
     * which are properly serialized.
     */
    int doCompaction(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, int gcBefore) throws IOException
    {
        // The collection of sstables passed may be empty (but not null); even if
        // it is not empty, it may compact down to nothing if all rows are deleted.
        Table table = cfs.getTable();
        if (DatabaseDescriptor.isSnapshotBeforeCompaction())
            table.snapshot("compact-" + cfs.columnFamily_);
        logger.info("Compacting [" + StringUtils.join(sstables, ",") + "]");
        String compactionFileLocation = table.getDataFileLocation(cfs.getExpectedCompactedFileSize(sstables));
        // If the compaction file path is null that means we have no space left for this compaction.
        // try again w/o the largest one.
        if (compactionFileLocation == null)
        {
            SSTableReader maxFile = cfs.getMaxSizeFile(sstables);
            List<SSTableReader> smallerSSTables = new ArrayList<SSTableReader>(sstables);
            smallerSSTables.remove(maxFile);
            return doCompaction(cfs, smallerSSTables, gcBefore);
        }

        // new sstables from flush can be added during a compaction, but only the compaction can remove them,
        // so in our single-threaded compaction world this is a valid way of determining if we're compacting
        // all the sstables (that existed when we started)
        boolean major = cfs.isCompleteSSTables(sstables);

        long startTime = System.currentTimeMillis();
        long totalkeysWritten = 0;

        // TODO the int cast here is potentially buggy
        int expectedBloomFilterSize = Math.max(SSTableReader.indexInterval(), (int)SSTableReader.getApproximateKeyCount(sstables));
        if (logger.isDebugEnabled())
          logger.debug("Expected bloom filter size : " + expectedBloomFilterSize);

        SSTableWriter writer;
        CompactionIterator ci = new CompactionIterator(sstables, gcBefore, major); // retain a handle so we can call close()
        Iterator<CompactionIterator.CompactedRow> nni = new FilterIterator(ci, PredicateUtils.notNullPredicate());
        executor.beginCompaction(cfs, ci);

        try
        {
            if (!nni.hasNext())
            {
                // don't mark compacted in the finally block, since if there _is_ nondeleted data,
                // we need to sync it (via closeAndOpen) first, so there is no period during which
                // a crash could cause data loss.
                cfs.markCompacted(sstables);
                return 0;
            }

            String newFilename = new File(compactionFileLocation, cfs.getTempSSTableFileName()).getAbsolutePath();
            writer = new SSTableWriter(newFilename, expectedBloomFilterSize, StorageService.getPartitioner());

            // validate the CF as we iterate over it
            AntiEntropyService.IValidator validator = AntiEntropyService.instance.getValidator(table.name, cfs.getColumnFamilyName(), null, major);
            validator.prepare();
            while (nni.hasNext())
            {
                CompactionIterator.CompactedRow row = nni.next();
                writer.append(row.key, row.buffer);
                validator.add(row);
                totalkeysWritten++;
            }
            validator.complete();
        }
        finally
        {
            ci.close();
        }

        SSTableReader ssTable = writer.closeAndOpenReader(DatabaseDescriptor.getKeysCachedFraction(table.name, cfs.getColumnFamilyName()));
        cfs.replaceCompactedSSTables(sstables, Arrays.asList(ssTable));
        submitMinorIfNeeded(cfs);

        String format = "Compacted to %s.  %d/%d bytes for %d keys.  Time: %dms.";
        long dTime = System.currentTimeMillis() - startTime;
        logger.info(String.format(format, writer.getFilename(), SSTable.getTotalBytes(sstables), ssTable.length(), totalkeysWritten, dTime));
        return sstables.size();
    }

    /**
     * This function is used to do the anti compaction process , it spits out the file which has keys that belong to a given range
     * If the target is not specified it spits out the file as a compacted file with the unecessary ranges wiped out.
     *
     * @param cfs
     * @param sstables
     * @param ranges
     * @param target
     * @return
     * @throws java.io.IOException
     */
    private List<SSTableReader> doAntiCompaction(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, Collection<Range> ranges, InetAddress target)
            throws IOException
    {
        Table table = cfs.getTable();
        logger.info("AntiCompacting [" + StringUtils.join(sstables, ",") + "]");
        // Calculate the expected compacted filesize
        long expectedRangeFileSize = cfs.getExpectedCompactedFileSize(sstables) / 2;
        String compactionFileLocation = table.getDataFileLocation(expectedRangeFileSize);
        if (compactionFileLocation == null)
        {
            throw new UnsupportedOperationException("disk full");
        }
        if (target != null)
        {
            // compacting for streaming: send to subdirectory
            compactionFileLocation = compactionFileLocation + File.separator + DatabaseDescriptor.STREAMING_SUBDIR;
        }
        List<SSTableReader> results = new ArrayList<SSTableReader>();

        long startTime = System.currentTimeMillis();
        long totalkeysWritten = 0;

        int expectedBloomFilterSize = Math.max(SSTableReader.indexInterval(), (int)(SSTableReader.getApproximateKeyCount(sstables) / 2));
        if (logger.isDebugEnabled())
          logger.debug("Expected bloom filter size : " + expectedBloomFilterSize);

        SSTableWriter writer = null;
        CompactionIterator ci = new AntiCompactionIterator(sstables, ranges, getDefaultGCBefore(), cfs.isCompleteSSTables(sstables));
        Iterator<CompactionIterator.CompactedRow> nni = new FilterIterator(ci, PredicateUtils.notNullPredicate());
        executor.beginCompaction(cfs, ci);

        try
        {
            if (!nni.hasNext())
            {
                return results;
            }

            while (nni.hasNext())
            {
                CompactionIterator.CompactedRow row = nni.next();
                if (writer == null)
                {
                    FileUtils.createDirectory(compactionFileLocation);
                    String newFilename = new File(compactionFileLocation, cfs.getTempSSTableFileName()).getAbsolutePath();
                    writer = new SSTableWriter(newFilename, expectedBloomFilterSize, StorageService.getPartitioner());
                }
                writer.append(row.key, row.buffer);
                totalkeysWritten++;
            }
        }
        finally
        {
            ci.close();
        }

        if (writer != null)
        {
            results.add(writer.closeAndOpenReader(DatabaseDescriptor.getKeysCachedFraction(table.name, cfs.getColumnFamilyName())));
            String format = "AntiCompacted to %s.  %d/%d bytes for %d keys.  Time: %dms.";
            long dTime = System.currentTimeMillis() - startTime;
            logger.info(String.format(format, writer.getFilename(), SSTable.getTotalBytes(sstables), results.get(0).length(), totalkeysWritten, dTime));
        }

        return results;
    }

    /**
     * This function goes over each file and removes the keys that the node is not responsible for
     * and only keeps keys that this node is responsible for.
     *
     * @throws IOException
     */
    private void doCleanupCompaction(ColumnFamilyStore cfs) throws IOException
    {
        Collection<SSTableReader> originalSSTables = cfs.getSSTables();
        List<SSTableReader> sstables = doAntiCompaction(cfs, originalSSTables, StorageService.instance.getLocalRanges(cfs.getTable().name), null);
        if (!sstables.isEmpty())
        {
            cfs.replaceCompactedSSTables(originalSSTables, sstables);
        }
    }

    /**
     * Performs a readonly "compaction" of all sstables in order to validate complete rows,
     * but without writing the merge result
     */
    private void doReadonlyCompaction(ColumnFamilyStore cfs, InetAddress initiator) throws IOException
    {
        Collection<SSTableReader> sstables = cfs.getSSTables();
        CompactionIterator ci = new CompactionIterator(sstables, getDefaultGCBefore(), true);
        executor.beginCompaction(cfs, ci);
        try
        {
            Iterator<CompactionIterator.CompactedRow> nni = new FilterIterator(ci, PredicateUtils.notNullPredicate());

            // validate the CF as we iterate over it
            AntiEntropyService.IValidator validator = AntiEntropyService.instance.getValidator(cfs.getTable().name, cfs.getColumnFamilyName(), initiator, true);
            validator.prepare();
            while (nni.hasNext())
            {
                CompactionIterator.CompactedRow row = nni.next();
                validator.add(row);
            }
            validator.complete();
        }
        finally
        {
            ci.close();
        }
    }

    /**
     * Group files of similar size into buckets.
     * @return A map of bucket level (relative size of individual sstables in the bucket) to the bucket itself.
     */
    static NavigableMap<Integer,List<SSTableReader>> getCompactionBuckets(Iterable<SSTableReader> files, int mergeFactor)
    {
        // gather into buckets by log size
        NavigableMap<Integer, List<SSTableReader>> buckets = new TreeMap<Integer, List<SSTableReader>>();
        int minlevel = 1+(int)(Math.log(MIN_BUCKET_SIZE) / Math.log(mergeFactor));
        for (SSTableReader sstable : files)
        {
            // level = log base mergeFactor of the filesize
            int level = Math.max((int)(Math.log(sstable.length()) / Math.log(mergeFactor)),
                                 minlevel);

            List<SSTableReader> bucket = buckets.get(level);
            if (bucket == null)
            {
                bucket = new ArrayList<SSTableReader>();
                buckets.put(level, bucket);
            }
            bucket.add(sstable);
        }

        return buckets;
    }

    public static int getDefaultGCBefore()
    {
        return (int)(System.currentTimeMillis() / 1000) - DatabaseDescriptor.getGcGraceInSeconds();
    }

    private static class AntiCompactionIterator extends CompactionIterator
    {
        private Set<SSTableScanner> scanners;

        public AntiCompactionIterator(Collection<SSTableReader> sstables, Collection<Range> ranges, int gcBefore, boolean isMajor)
                throws IOException
        {
            super(getCollatedRangeIterator(sstables, ranges), gcBefore, isMajor);
        }

        private static Iterator getCollatedRangeIterator(Collection<SSTableReader> sstables, final Collection<Range> ranges)
                throws IOException
        {
            org.apache.commons.collections.Predicate rangesPredicate = new org.apache.commons.collections.Predicate()
            {
                public boolean evaluate(Object row)
                {
                    return Range.isTokenInRanges(((IteratingRow)row).getKey().token, ranges);
                }
            };
            CollatingIterator iter = FBUtilities.<IteratingRow>getCollatingIterator();
            int perFileBuffer = FILE_BUFFER_SIZE / Math.max(1, sstables.size());
            for (SSTableReader sstable : sstables)
            {
                SSTableScanner scanner = sstable.getScanner(perFileBuffer);
                iter.addIterator(new FilterIterator(scanner, rangesPredicate));
            }
            return iter;
        }

        public Iterable<SSTableScanner> getScanners()
        {
            if (scanners == null)
            {
                scanners = new HashSet<SSTableScanner>();
                for (Object o : ((CollatingIterator)source).getIterators())
                {
                    scanners.add((SSTableScanner)((FilterIterator)o).getIterator());
                }
            }
            return scanners;
        }
    }

    public void checkAllColumnFamilies() throws IOException
    {
        // perform estimates
        for (final ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            Runnable runnable = new Runnable()
            {
                public void run ()
                {
                    int mergeFactor = getMergeFactor(cfs);
                    logger.debug("Estimating compactions for " + cfs.columnFamily_);
                    Collection<List<SSTableReader>> buckets = getCompactionBuckets(cfs.getSSTables(), mergeFactor).values();
                    updateEstimateFor(cfs, buckets, mergeFactor);
                }
            };
            executor.submit(runnable);
        }

        // actually schedule compactions.  done in a second pass so all the estimates occur before we
        // bog down the executor in actual compactions.
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            submitMinorIfNeeded(cfs);
        }
    }

    private class CompactionExecutor extends DebuggableThreadPoolExecutor
    {
        private volatile ColumnFamilyStore cfs;
        private volatile CompactionIterator ci;

        public CompactionExecutor()
        {
            super("COMPACTION-POOL");
        }

        @Override
        public void afterExecute(Runnable r, Throwable t)
        {
            super.afterExecute(r, t);
            cfs = null;
            ci = null;
        }

        void beginCompaction(ColumnFamilyStore cfs, CompactionIterator ci)
        {
            this.cfs = cfs;
            this.ci = ci;
        }

        public String getColumnFamilyName()
        {
            return cfs == null ? null : cfs.getColumnFamilyName();
        }

        public Long getBytesTotal()
        {
            return ci == null ? null : ci.getTotalBytes();
        }

        public Long getBytesCompleted()
        {
            return ci == null ? null : ci.getBytesRead();
        }
    }

    public String getColumnFamilyInProgress()
    {
        return executor.getColumnFamilyName();
    }

    public Long getBytesTotalInProgress()
    {
        return executor.getBytesTotal();
    }

    public Long getBytesCompacted()
    {
        return executor.getBytesCompleted();
    }

    public int getPendingTasks()
    {
        int n = 0;
        for (Integer i : estimatedCompactions.values())
        {
            n += i;
        }
        return n;
    }
}
