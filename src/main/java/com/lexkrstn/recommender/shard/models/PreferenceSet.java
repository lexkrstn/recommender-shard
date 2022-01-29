package com.lexkrstn.recommender.shard.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

import java.util.Set;
import java.util.TreeSet;

/**
 * Encapsulates all the items liked by a user.
 */
@Data
@AllArgsConstructor
@ToString
public class PreferenceSet implements Cloneable {
    /**
     * User or compilation ID.
     */
    private final long ownerId;

    /**
     * The number of entitity IDs that can be stored
     * in the data source without the need of its extension.
     */
    private int capacity;

    /**
     * Array of the IDs of the liked items ("entities").
     */
    private Set<Long> entityIds;

    /**
     * Offset of the entity in the data source.
     * Warning! It's valid until any change to the source.
     */
    private final long offset;

    /**
     * Creates a new preference set containing the preference.
     * The capacity and offset are set to 0.
     */
    public static PreferenceSet fromPreference(Preference preference) {
        Set<Long> entityIds = new TreeSet<>();
        entityIds.add(preference.getEntityId());
        return new PreferenceSet(preference.getOwnerId(), 0, entityIds, 0);
    }

    /**
     * Returns a boolean value indicating whether the entity
     * exists in the set.
     *
     * @param entityId Liked item ID.
     */
    public boolean has(long entityId) {
        return entityIds.contains(entityId);
    }

    /**
     * Returns the number of liked items.
     */
    public int getEntityCount() {
        return entityIds.size();
    }

    /**
     * Adds the liked item to the set.
     *
     * @param entityId Liked item ID.
     * @return A boolean value indicating whether a new entity has been added.
     *         It's false if the entity is already in there.
     */
    public boolean add(long entityId) {
        return entityIds.add(entityId);
    }

    /**
     * Removes the liked item from the set.
     *
     * @param entityId Liked item ID.
     * @return A boolean value indicating whether the entity has been removed.
     *         It's false if the entity isn't found in there.
     */
    public boolean remove(long entityId) {
        return entityIds.remove(entityId);
    }

    /**
     * Returns a copy of this object.
     */
    @Override
    public Object clone() {
        final PreferenceSet clone;
        try {
            clone = (PreferenceSet) super.clone();
        }
        catch (CloneNotSupportedException ex) {
            throw new RuntimeException("superclass messed up", ex);
        }
        clone.entityIds = new TreeSet<>(entityIds);
        return clone;
    }

    /**
     * Computes the similarity rate of the set with another one.
     *
     * Given like sets A and B,
     * Similarity rate = | A ^ B | / | A v B | * 100%
     *
     * @param other Another set to compare with.
     * @return A floating-point value from 0 to 100.
     */
    public float getSimilarityWith(PreferenceSet other) {
        int conjunction = 0;
        for (var a : entityIds) {
            if (other.entityIds.contains(a)) {
                conjunction++;
            }
        }
        int disjunction = entityIds.size() + other.entityIds.size() - conjunction;
        return disjunction == 0 ? 0.0f : 100.0f * conjunction / disjunction;
    }
}
