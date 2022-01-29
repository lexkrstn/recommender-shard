package com.lexkrstn.recommender.shard.models;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Recommendation {
    private final long entityId;
    private final float weight;
}
