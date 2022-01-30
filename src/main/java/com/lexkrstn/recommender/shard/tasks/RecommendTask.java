package com.lexkrstn.recommender.shard.tasks;

import com.lexkrstn.recommender.shard.models.PreferenceSet;
import com.lexkrstn.recommender.shard.models.Recommendation;
import com.lexkrstn.recommender.shard.models.SimilarityTable;

import java.util.List;

/**
 * The task that creates recommendation list for a preference set.
 */
public class RecommendTask extends AbstractTask {
    private static final int SIMILARITY_TABLE_SIZE = 1000;

    private final long ownerId;
    private PreferenceSet preferenceSet;
    private boolean firstCycle = true;
    private SimilarityTable similarityTable;

    /**
     * @param ownerId The ID of the preference set to recommend to.
     */
    public RecommendTask(long ownerId) {
        this.ownerId = ownerId;
    }

    @Override
    public void processPreferenceSet(PreferenceSet preferenceSet) {
        if (firstCycle && preferenceSet.getOwnerId() == ownerId) {
            // In the first cycle we just find the preference set
            this.preferenceSet = preferenceSet;
            similarityTable = new SimilarityTable(preferenceSet, SIMILARITY_TABLE_SIZE);
        } else if (!firstCycle && this.preferenceSet != null) {
            // In the second cycle we build the recommendation table
            similarityTable.process(preferenceSet);
        }
    }

    @Override
    public boolean proceedPass() {
        if (firstCycle) {
            firstCycle = false;
            return true;
        }
        complete();
        return false;
    }

    /**
     * Returns recommendation list sorted by weight in descending order.
     */
    public List<Recommendation> getRecommendationList() {
        return similarityTable.getRecommendationList();
    }
}
