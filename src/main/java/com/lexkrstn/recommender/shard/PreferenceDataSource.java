package com.lexkrstn.recommender.shard;

import java.io.IOException;
import java.util.List;

/**
 * recommendation data source.
 */
public interface PreferenceDataSource extends AutoCloseable {
    /**
     * Returns the last change time in milliseconds.
     *
     * If data two sources differ this value can be used to
     * determine the most "fresh" one.
     */
    long getChangeTimeMillis() throws IOException;

    /**
     * Returns the number of preference sets stored in the source.
     */
    long getPreferenceSetCount() throws IOException;

    /**
     * Returns the total number of preferences stored in the source.
     */
    long getPreferenceCount() throws IOException;

    /**
     * Seeks to the first preference.
     */
    void rewind() throws IOException;

    /**
     * Tries to rewrite the preference set without extending the file.
     *
     * @param preferenceSet New preference set object.
     * @return A boolean value indicating whether the try was successful.
     */
    boolean tryQuickRewrite(PreferenceSet preferenceSet) throws IOException;

    /**
     * Performs the preference sets shrinking the file.
     */
    void delete(List<PreferenceSet> preferenceSets) throws IOException;

    /**
     * Adds the preference sets extending the file.
     */
    void add(List<PreferenceSet> preferenceSets) throws IOException;

    /**
     * Persists any in-memory cached data.
     */
    void flush() throws IOException;

    /**
     * Returns true only if there is another preference set to read with the next() method.
     */
    boolean hasNext() throws IOException;

    /**
     * Reads the next preference set object from the source.
     */
    PreferenceSet next() throws IOException;
}
