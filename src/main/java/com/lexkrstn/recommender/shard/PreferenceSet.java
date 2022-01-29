package com.lexkrstn.recommender.shard;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Arrays;

@Getter
@AllArgsConstructor
@EqualsAndHashCode
public class PreferenceSet implements Cloneable {
    private final long ownerId;
    private final int capacity;
    private long[] entityIds;
    private final long offset;

    public boolean has(long entityId) {
        return Arrays.binarySearch(entityIds, entityId) >= 0;
    }

    public int getPreferenceCount() {
        return entityIds.length;
    }

    public void add(long entityId) {
        entityIds = Arrays.copyOf(entityIds, entityIds.length + 1);
        entityIds[entityIds.length - 1] = entityId;
        Arrays.sort(entityIds);
    }

    public void remove(long entityId) {
        if (!has(entityId)) return;
        long[] newentityIds = new long[entityIds.length - 1];
        for (int i = 0, j = 0; i < entityIds.length; i++) {
            if (entityIds[i] != entityId) {
                newentityIds[j++] = entityIds[i];
            }
        }
        entityIds = newentityIds;
    }

    @Override
    public Object clone() {
        return new PreferenceSet(ownerId, capacity,
                Arrays.copyOf(entityIds, entityIds.length), offset);
    }

    public float getSimilarityWith(PreferenceSet other) {
        // TODO
        return 0;
    }
}
