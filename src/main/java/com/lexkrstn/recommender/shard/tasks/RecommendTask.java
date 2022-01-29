package com.lexkrstn.recommender.shard.tasks;

import com.lexkrstn.recommender.shard.models.PreferenceSet;
import com.lexkrstn.recommender.shard.models.Recommendation;

import java.util.List;

public class RecommendTask extends RecommenderTask {
    private final long ownerId;
    private PreferenceSet preferenceSet;
    private boolean firstCycle = true;
    private List<Recommendation> recommendationList;

    public RecommendTask(long ownerId) {
        this.ownerId = ownerId;
    }

    @Override
    public void processPreferenceSet(PreferenceSet preferenceSet) {
        if (firstCycle && preferenceSet.getOwnerId() == ownerId) {
            // In the first cycle we just find the preference set
            this.preferenceSet = preferenceSet;
        } else if (!firstCycle && this.preferenceSet != null) {
            // In the second cycle we build the recommendation table
            // TODO
        }
    }

    @Override
    public boolean proceedPass() {
        if (firstCycle) {
            firstCycle = false;
            return true;
        }
        buildRecommendationList();
        complete();
        return false;
    }

    public List<Recommendation> getRecommendationList() {
        return recommendationList;
    }

    private void buildRecommendationList() {
        // TODO
    }
}
