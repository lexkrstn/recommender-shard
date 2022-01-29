package com.lexkrstn.recommender.shard.tasks;

import com.lexkrstn.recommender.shard.io.PreferenceChangeBulk;
import com.lexkrstn.recommender.shard.models.PreferenceSet;
import com.lexkrstn.recommender.shard.models.Preference;

/**
 * The task that removes a preference from a set (if it is there yet).
 */
public class UnpreferTask extends RecommenderTask implements WithPreference {
    private Preference preference;
    private PreferenceChangeBulk changeBulk;
    private boolean affected = false;

    public UnpreferTask(Preference preference, PreferenceChangeBulk changeBulk) {
        this.preference = preference;
        this.changeBulk = changeBulk;
    }

    @Override
    public Preference getPreference() {
        return preference;
    }

    @Override
    public void processPreferenceSet(PreferenceSet preferenceSet) {
        if (preference.getOwnerId() == preferenceSet.getOwnerId()) {
            if (preferenceSet.has(preference.getEntityId())) {
                changeBulk.removePreference(preferenceSet, preference.getEntityId());
                affected = true;
            }
        }
    }

    /**
     * Returns true if the preference has existed in the data source.
     */
    public boolean hasAffected() {
        return affected;
    }
}
