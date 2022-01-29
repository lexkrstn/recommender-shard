package com.lexkrstn.recommender.shard.io;

import com.lexkrstn.recommender.shard.models.PreferenceSet;
import lombok.Data;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;


/**
 * File implementation of PreferenceDataSource.
 */
public class PreferenceFile implements PreferenceDataSource {
    /**
     * File header descriptor.
     */
    @Data
    private static class Header {
        /**
         * File prefix "PREF".
         */
        public static final byte[] PREFIX = { 0x50, 0x52, 0x45, 0x46 };
        /**
         * Header size in bytes.
         */
        public static final int SIZE = PREFIX.length + 1 + 8 * 4;
        /**
         * Current file structure version.
         */
        public static final byte VERSION = 1;
        /**
         * File structure version.
         */
        private byte version;
        /**
         * Last file change time.
         */
        private long changeTimeMillis;
        private long preferenceSetCount;
        private long preferenceCount;
        private long dataSize;
    }

    /**
     * Path to the file.
     */
    private final String filePath;
    private final int minCapacity = 2;
    private final RandomAccessFile file;
    private final Header header = new Header();
    private final LinkedList<PreferenceSet> lastPreferenceSets = new LinkedList<>();
    private final long maxLastPreferenceSets = 100;
    private final byte[] moveBuffer = new byte[10240];
    private long preferenceSetIndex = -1;

    /**
     * @param filePath The path of the file.
     * @throws FileNotFoundException If the file hasn't been found.
     * @throws SecurityException If the file cannot be opened in "rw" mode.
     * @throws IOException Any other IO error.
     */
    public PreferenceFile(String filePath) throws IOException {
        this.filePath = filePath;
        file = new RandomAccessFile(filePath, "rw");
        if (file.length() == 0) {
            writeHeader();
        } else {
            readHeader();
        }
    }

    /**
     * Returns the size (in bytes) of the preference set stored in the file.
     */
    private static long getPreferenceSetSize(PreferenceSet preferenceSet) {
        return 8L * (preferenceSet.getCapacity() + 1L) + 4L * 2L;
    }

    /**
     * Calculates new capacity of a preference set.
     */
    private int getNewCapacity(PreferenceSet preferenceSet) {
        return minCapacity * (preferenceSet.getEntityCount() / minCapacity) + minCapacity;
    }

    private void readHeader() throws IOException {
        file.seek(0);

        byte[] prefix = new byte[Header.PREFIX.length];
        if (file.read(prefix) != prefix.length || !Arrays.equals(Header.PREFIX, prefix)) {
            throw new IOException("The file " + filePath + " is not a valid preference file");
        }

        header.setVersion(file.readByte());
        header.setChangeTimeMillis(file.readLong());
        header.setPreferenceSetCount(file.readLong());
        header.setPreferenceCount(file.readLong());
        header.setDataSize(file.readLong());

        preferenceSetIndex = 0;
    }

    private void writeHeader() throws IOException {
        file.seek(0);

        file.write(Header.PREFIX);
        file.writeByte(Header.VERSION);
        file.writeLong(header.getChangeTimeMillis());
        file.writeLong(header.getPreferenceSetCount());
        file.writeLong(header.getPreferenceCount());
        file.writeLong(header.getDataSize());

        preferenceSetIndex = 0;
    }

    private void writePreferenceSet(PreferenceSet preferenceSet, boolean isNew) throws IOException {
        file.writeLong(preferenceSet.getOwnerId());
        file.writeInt(preferenceSet.getCapacity());
        file.writeInt(preferenceSet.getEntityCount());
        for (var entityId : preferenceSet.getEntityIds()) {
            file.writeLong(entityId);
        }
        if (isNew) {
            for (long i = preferenceSet.getEntityCount(); i < preferenceSet.getCapacity(); i++) {
                file.writeLong(0);
            }
        } else if (preferenceSet.getEntityCount() < preferenceSet.getCapacity()) {
            file.skipBytes((preferenceSet.getCapacity() - preferenceSet.getEntityCount()) * 8);
        }
        preferenceSetIndex++;
    }

    private PreferenceSet readPreferenceSet() throws IOException {
        if (preferenceSetIndex >= getPreferenceSetCount()) {
            return null;
        }
        long offset = file.getFilePointer();
        long ownerId = file.readLong();
        int entityIdsCapacity = file.readInt();
        int entityIdCount = file.readInt();
        Set<Long> entityIds = new TreeSet<>();
        for (int j = 0; j < entityIdCount; j++) {
            entityIds.add(file.readLong());
        }
        if (entityIdsCapacity > entityIdCount) {
            file.skipBytes((entityIdsCapacity - entityIdCount) * 8);
        }
        var preferenceSet = new PreferenceSet(ownerId, entityIdsCapacity, entityIds, offset);
        preferenceSetIndex++;
        return preferenceSet;
    }

    private void readPreferenceSets() throws IOException {
        for (long i = 0; i < maxLastPreferenceSets; i++) {
            var preferenceSet = readPreferenceSet();
            if (preferenceSet == null) break;
            lastPreferenceSets.add(preferenceSet);
        }
    }

    @Override
    public void close() throws Exception {
        flush();
        file.close();
    }

    @Override
    public void flush() throws IOException {
        writeHeader();
    }

    @Override
    public long getChangeTimeMillis() {
        return header.getChangeTimeMillis();
    }

    @Override
    public long getPreferenceSetCount() {
        return header.getPreferenceSetCount();
    }

    @Override
    public long getPreferenceCount() {
        return header.getPreferenceCount();
    }

    @Override
    public void rewind() throws IOException {
        file.seek(Header.SIZE);
        preferenceSetIndex = 0;
    }

    @Override
    public boolean tryQuickRewrite(PreferenceSet originalPreferenceSet,
                                   PreferenceSet newPreferenceSet) throws IOException {
        if (originalPreferenceSet.getCapacity() < newPreferenceSet.getEntityCount()) {
            return false;
        }
        final long offset = file.getFilePointer();
        file.seek(originalPreferenceSet.getOffset());
        writePreferenceSet(newPreferenceSet, false);
        file.seek(offset);
        header.setChangeTimeMillis(Calendar.getInstance().getTimeInMillis());
        header.setPreferenceCount(header.getPreferenceSetCount()
                - originalPreferenceSet.getEntityCount()
                + newPreferenceSet.getEntityCount());
        return true;
    }

    @Override
    public void delete(List<PreferenceSet> preferenceSets) throws IOException {
        final var sortedSets = preferenceSets.stream()
                .sorted(Comparator.comparingLong(PreferenceSet::getOffset))
                .toList();
        final long[] holeOffsets = sortedSets.stream()
                .mapToLong(PreferenceSet::getOffset)
                .toArray();
        final long[] holeSizes = sortedSets.stream()
                .mapToLong(PreferenceFile::getPreferenceSetSize)
                .toArray();
        final long fileSize = header.getDataSize() + Header.SIZE;
        long totalHoleSize = 0;
        long offsetChange = 0;
        for (int i = 0; i < holeOffsets.length; i++) {
            long to = holeOffsets[i] + offsetChange;
            long from = holeOffsets[i] + holeSizes[i];
            long end = i + 1 < holeOffsets.length
                ? holeOffsets[i + 1]
                : fileSize;
            move(to, from, end - from);
            offsetChange += to - from;
            totalHoleSize += holeSizes[i];
        }
        // Update header
        final var deletedPreferenceCount = preferenceSets.stream()
                .map(PreferenceSet::getEntityCount)
                .reduce(0, Integer::sum);
        header.setPreferenceCount(header.getPreferenceCount() - deletedPreferenceCount);
        header.setPreferenceSetCount(header.getPreferenceSetCount() - preferenceSets.size());
        header.setChangeTimeMillis(Calendar.getInstance().getTimeInMillis());
        header.setDataSize(header.getDataSize() - totalHoleSize);
    }

    /**
     * Copies the values of `size` bytes from the location in the file pointed
     * by `from` to the memory block pointed by `to`.
     * Copying takes place as if an intermediate buffer were used, allowing the
     * destination and source to overlap.
     *
     * @param to Destination offset in bytes.
     * @param from Source offset in bytes.
     * @param size Block size to copy in bytes.
     */
    private void move(long to, long from, long size) throws IOException {
        long numChunks = size / moveBuffer.length;
        if (size % moveBuffer.length > 0) {
            numChunks++;
        }
        long offset = 0;
        for (long i = 0; i < numChunks; i++) {
            int chunkSize = i + 1 < numChunks
                ? moveBuffer.length
                : (int)(size % moveBuffer.length);
            file.seek(from + offset);
            file.read(moveBuffer, 0, chunkSize);
            file.seek(to + offset);
            file.write(moveBuffer, 0, chunkSize);
            offset += chunkSize;
        }
    }

    @Override
    public void add(List<PreferenceSet> preferenceSets) throws IOException {
        file.seek(Header.SIZE + header.getDataSize());
        long dataSizeChange = 0;
        long preferenceCount = 0;
        for (var preferenceSet : preferenceSets) {
            if (preferenceSet.getCapacity() < preferenceSet.getEntityCount()) {
                preferenceSet.setCapacity(getNewCapacity(preferenceSet));
            }
            writePreferenceSet(preferenceSet, true);
            dataSizeChange += getPreferenceSetSize(preferenceSet);
            preferenceCount += preferenceSet.getEntityCount();
        }
        // Update header
        header.setPreferenceCount(header.getPreferenceCount() + preferenceCount);
        header.setPreferenceSetCount(header.getPreferenceSetCount() + preferenceSets.size());
        header.setChangeTimeMillis(Calendar.getInstance().getTimeInMillis());
        header.setDataSize(header.getDataSize() + dataSizeChange);
    }

    @Override
    public boolean hasNext() throws IOException {
        if (lastPreferenceSets.isEmpty()) {
            readPreferenceSets();
        }
        return !lastPreferenceSets.isEmpty();
    }

    @Override
    public PreferenceSet next() throws IOException {
        if (lastPreferenceSets.isEmpty()) {
            readPreferenceSets();
        }
        return lastPreferenceSets.pollFirst();
    }
}
