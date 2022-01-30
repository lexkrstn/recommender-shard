package com.lexkrstn.recommender.shard.tasks;

import com.lexkrstn.recommender.shard.io.PreferenceChangeBulk;
import com.lexkrstn.recommender.shard.models.PreferenceSet;
import com.lexkrstn.recommender.shard.models.Preference;

/**
 * The task that adds a preference to a set (if it isn't there yet).
 */
public class PreferTask extends AbstractTask implements WithPreference {
    private final Preference preference;
    private final PreferenceChangeBulk changeBulk;
    private boolean found = false;
    private boolean exists = false;

    public PreferTask(Preference preference, PreferenceChangeBulk changeBulk) {
        this.preference = preference;
        this.changeBulk = changeBulk;
    }

    @Override
    public void processPreferenceSet(PreferenceSet preferenceSet) {
        if (preference.getOwnerId() == preferenceSet.getOwnerId()) {
            if (preferenceSet.has(preference.getEntityId())) {
                exists = true;
            } else {
                changeBulk.addPreference(preferenceSet, preference.getEntityId());
            }
            found = true;
        }
    }

    @Override
    public boolean proceedPass() {
        if (!found) {
            changeBulk.addPreference(preference);
        }
        return super.proceedPass();
    }

    @Override
    public Preference getPreference() {
        return preference;
    }

    /**
     * Returns true if the preference hasn't existed in the data source.
     */
    public boolean hasAdded() {
        return !exists;
    }
}
