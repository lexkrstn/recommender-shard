package com.lexkrstn.recommender.shard;

import lombok.Data;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;


/**
 * File implementation of PreferenceDataSource.
 */
public class PreferenceFile implements PreferenceDataSource {
    /**
     * File header.
     */
    @Data
    private static class Header {
        public static final byte[] PREFIX = { 0x50, 0x52, 0x45, 0x46 };
        public static final int SIZE = PREFIX.length + 8 * 3;
        private long changeTimeMillis;
        private long preferenceSetCount;
        private long preferenceCount;
    }

    private final String filePath;
    private final RandomAccessFile file;
    private final Header header = new Header();
    private final LinkedList<PreferenceSet> lastPreferenceSets = new LinkedList<>();
    private final long maxLastPreferenceSets = 100;
    private final byte[] moveBuffer = new byte[10240];
    private long offset = 0;
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

    private static long getPreferenceSetSize(int entityIdsCapacity) {
        return 8L * (entityIdsCapacity + 1L) + 4L * 2L;
    }

    private void readHeader() throws IOException {
        file.seek(0);

        byte[] prefix = new byte[Header.PREFIX.length];
        if (file.read(prefix) != prefix.length || !Arrays.equals(Header.PREFIX, prefix)) {
            throw new IOException("The file " + filePath + " is not a valid preference file");
        }

        header.setChangeTimeMillis(file.readLong());
        header.setPreferenceSetCount(file.readLong());
        header.setPreferenceCount(file.readLong());

        offset = Header.SIZE;
        preferenceSetIndex = 0;
    }

    private void writeHeader() throws IOException {
        file.seek(0);
        file.write(Header.PREFIX);
        file.writeLong(header.getChangeTimeMillis());
        file.writeLong(header.getPreferenceSetCount());
        file.writeLong(header.getPreferenceCount());

        offset = Header.SIZE;
        preferenceSetIndex = 0;
    }

    private void writePreferenceSet(PreferenceSet preferenceSet) throws IOException {
        file.writeLong(preferenceSet.getOwnerId());
        file.writeInt(preferenceSet.getCapacity());
        file.writeInt(preferenceSet.getPreferenceCount());
        long[] entityIds = preferenceSet.getEntityIds();
        for (int i = 0; i < preferenceSet.getPreferenceCount(); i++) {
            file.writeLong(entityIds[i]);
        }
        if (preferenceSet.getCapacity() > preferenceSet.getPreferenceCount()) {
            file.skipBytes(preferenceSet.getCapacity() - preferenceSet.getPreferenceCount());
        }
        offset += getPreferenceSetSize(preferenceSet.getCapacity());
        preferenceSetIndex++;
    }

    private PreferenceSet readPreferenceSet() throws IOException {
        if (preferenceSetIndex >= getPreferenceSetCount()) {
            return null;
        }
        long ownerId = file.readLong();
        int entityIdsCapacity = file.readInt();
        int entityIdCount = file.readInt();
        long[] entityIds = new long[entityIdCount];
        for (int j = 0; j < entityIdCount; j++) {
            entityIds[j] = file.readLong();
        }
        if (entityIdsCapacity > entityIdCount) {
            file.skipBytes(entityIdsCapacity - entityIdCount);
        }
        var preferenceSet = new PreferenceSet(ownerId, entityIdsCapacity, entityIds, offset);
        offset += getPreferenceSetSize(entityIdsCapacity);
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
        offset = Header.SIZE;
        preferenceSetIndex = 0;
    }

    @Override
    public boolean tryQuickRewrite(PreferenceSet preferenceSet) throws IOException {
        if (preferenceSet.getCapacity() < preferenceSet.getPreferenceCount()) {
            return false;
        }
        file.seek(file.getFilePointer());
        writePreferenceSet(preferenceSet);
        file.seek(offset);
        return true;
    }

    @Override
    public void delete(List<PreferenceSet> preferenceSets) throws IOException {
        var sortedSets = preferenceSets.stream()
                .sorted(Comparator.comparingLong(PreferenceSet::getOffset))
                .toList();
        long[] holeOffsets = sortedSets.stream()
                .mapToLong(PreferenceSet::getOffset)
                .toArray();
        long[] holeSizes = sortedSets.stream()
                .mapToLong(ps -> getPreferenceSetSize(ps.getCapacity()))
                .toArray();
        long fileSize = file.length();
        long offsetChange = 0;
        for (int i = 0; i < holeOffsets.length; i++) {
            long to = holeOffsets[i] + offsetChange;
            long from = holeOffsets[i] + holeSizes[i];
            long end = i + 1 < holeOffsets.length
                ? holeOffsets[i + 1]
                : fileSize;
            move(to, from, end - from);
            offsetChange += to - from;
        }
        // Update header
        var deletedPreferenceCount = preferenceSets.stream()
                .map(PreferenceSet::getPreferenceCount)
                .reduce(0, Integer::sum);
        header.setPreferenceCount(header.getPreferenceCount() - deletedPreferenceCount);
        header.setPreferenceSetCount(header.getPreferenceSetCount() - preferenceSets.size());
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
    public void add(List<PreferenceSet> ownerIds) throws IOException {

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
