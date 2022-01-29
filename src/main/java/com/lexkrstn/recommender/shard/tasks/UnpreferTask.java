package com.lexkrstn.recommender.shard.tasks;

import com.lexkrstn.recommender.shard.PreferenceChangeBulk;
import com.lexkrstn.recommender.shard.PreferenceSet;

public class UnpreferTask extends RecommenderTask implements WithPreference {
    private Preference preference;
    private PreferenceChangeBulk changeBulk;

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
            if (!preferenceSet.has(preference.getEntityId())) {
                changeBulk.removePreference(preferenceSet, preference.getEntityId());
            }
        }
    }

    public boolean hasAffected() {
        return true;
    }
}
