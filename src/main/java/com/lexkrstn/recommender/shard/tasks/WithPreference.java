package com.lexkrstn.recommender.shard.tasks;

import com.lexkrstn.recommender.shard.models.Preference;

public interface WithPreference {
    Preference getPreference();
}
