package com.lexkrstn.recommender.shard.tasks;

import com.lexkrstn.recommender.shard.PreferenceDataSource;

/**
 * Represents a preference record stored in a data source.
 *
 * The record typically represents what the user likes or saves in his/her favorites.
 * The owner of the entity can be the user id, the id of favorites folder and things like that.
 * The entity id represents the entity user likes, e.g. the id of a book or music track.
 *
 * @see PreferenceDataSource
 */
public class Preference {
    protected final long ownerId;
    protected final long entityId;

    public Preference(long ownerId, long entityId) {
        this.ownerId = ownerId;
        this.entityId = entityId;
    }

    public long getOwnerId() {
        return ownerId;
    }

    public long getEntityId() {
        return entityId;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof Preference)) return false;
        if (other.getClass() != Preference.class) return other.equals(this);
        Preference preference = (Preference) other;
        return this.ownerId == preference.ownerId && this.entityId == preference.entityId;
    }
}
