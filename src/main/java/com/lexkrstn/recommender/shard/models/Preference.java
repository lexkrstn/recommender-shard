package com.lexkrstn.recommender.shard.models;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Represents a preference record stored in a data source.
 *
 * The record typically represents what the user likes or saves in his/her favorites.
 * The owner of the entity can be the user id, the id of favorites folder and things like that.
 * The entity id represents the entity user likes, e.g. the id of a book or music track.
 */
@Data
@AllArgsConstructor
public class Preference {
    protected final long ownerId;
    protected final long entityId;
}
