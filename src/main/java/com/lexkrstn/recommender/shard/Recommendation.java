package com.lexkrstn.recommender.shard;

public class Recommendation {
    private final long entityId;
    private final float weight;

    public Recommendation(long entityId, float weight) {
        this.entityId = entityId;
        this.weight = weight;
    }

    public long getEntityId() {
        return entityId;
    }

    public float getWeight() {
        return weight;
    }

}
