package com.lexkrstn.recommender.shard.tasks;

import com.lexkrstn.recommender.shard.PreferenceChangeBulk;
import com.lexkrstn.recommender.shard.PreferenceSet;

public class PreferTask extends RecommenderTask implements WithPreference {
    private Preference preference;
    private PreferenceChangeBulk changeBulk;

    public PreferTask(Preference preference, PreferenceChangeBulk changeBulk) {
        this.preference = preference;
        this.changeBulk = changeBulk;
    }

    @Override
    public void processPreferenceSet(PreferenceSet preferenceSet) {
        if (preference.getOwnerId() == preferenceSet.getOwnerId()) {
            if (!preferenceSet.has(preference.getEntityId())) {
                changeBulk.addPreference(preferenceSet, preference.getEntityId());
            }
        }
    }

    @Override
    public Preference getPreference() {
        return preference;
    }
}
