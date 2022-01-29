package com.lexkrstn.recommender.shard.tasks;

import com.lexkrstn.recommender.shard.PreferenceSet;
import lombok.Getter;

@Getter
public class GetPreferencesTask extends RecommenderTask {
    private final long ownerId;
    private PreferenceSet preferenceSet;

    public GetPreferencesTask(long ownerId) {
        this.ownerId = ownerId;
    }

    @Override
    public void processPreferenceSet(PreferenceSet preferenceSet) {
        if (preferenceSet.getOwnerId() == ownerId) {
            // In the first cycle we just find the preference set
            this.preferenceSet = preferenceSet;
        }
    }
}

