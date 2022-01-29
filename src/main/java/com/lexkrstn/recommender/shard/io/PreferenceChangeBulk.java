package com.lexkrstn.recommender.shard.io;

import com.lexkrstn.recommender.shard.models.Preference;
import com.lexkrstn.recommender.shard.models.PreferenceSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Records the like / dislike operations that extends or shrinks the file
 * to execute them at a time.
 */
public class PreferenceChangeBulk {
    private final Logger log = LoggerFactory.getLogger(PreferenceChangeBulk.class);
    private final PreferenceDataSource dataSource;
    /**
     * The preference sets unmodified from the last read.
     */
    private final HashMap<Long, PreferenceSet> originalPreferenceSets = new HashMap<>();
    /**
     * Modified preference sets.
     */
    private final HashMap<Long, PreferenceSet> preferenceSets = new HashMap<>();

    /**
     * Constructs a Bulk.
     *
     * @param dataSource The data storage.
     */
    public PreferenceChangeBulk(PreferenceDataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Records an operation of addition an entity to a preference set.
     *
     * @param preferenceSet The preference set to add into.
     * @param entityId ID of a liked item.
     */
    public void addPreference(PreferenceSet preferenceSet, long entityId) {
        var foundPreferenceSet= preferenceSets.get(preferenceSet.getOwnerId());
        if (foundPreferenceSet == null) {
            originalPreferenceSets.put(preferenceSet.getOwnerId(), preferenceSet);
            foundPreferenceSet = (PreferenceSet) preferenceSet.clone();
            preferenceSets.put(preferenceSet.getOwnerId(), foundPreferenceSet);
        }
        foundPreferenceSet.add(entityId);
        log.debug("Recorded adding liked item {} to existing {}",
                entityId, preferenceSet.getOwnerId());
    }

    /**
     * Records an operation of addition an entity to a NEW preference set.
     *
     * @param preference The preference to add.
     */
    public void addPreference(Preference preference) {
        var foundPreferenceSet= preferenceSets.get(preference.getOwnerId());
        if (foundPreferenceSet == null) {
            foundPreferenceSet = PreferenceSet.fromPreference(preference);
            preferenceSets.put(preference.getOwnerId(), foundPreferenceSet);
        }
        foundPreferenceSet.add(preference.getEntityId());
        log.debug("Recorded adding liked item {} to new {}",
                preference.getEntityId(), preference.getOwnerId());
    }

    /**
     * Records an operation of removal an entity to a preference set.
     *
     * @param preferenceSet The preference set to add into.
     * @param entityId ID of a liked item.
     */
    public void removePreference(PreferenceSet preferenceSet, long entityId) {
        var foundPreferenceSet= preferenceSets.get(preferenceSet.getOwnerId());
        if (foundPreferenceSet == null) {
            originalPreferenceSets.put(preferenceSet.getOwnerId(), preferenceSet);
            foundPreferenceSet = (PreferenceSet) preferenceSet.clone();
            preferenceSets.put(preferenceSet.getOwnerId(), foundPreferenceSet);
        }
        foundPreferenceSet.remove(entityId);
        log.debug("Recorded removal liked item {} from {}",
                entityId, preferenceSet.getOwnerId());
    }

    /**
     * Executes the previously recorded operations of addition and/or removal of preferences.
     */
    public void execute() throws IOException {
        List<PreferenceSet> changedSets = preferenceSets.values()
                .stream()
                .filter(ps -> {
                    var originalPs = originalPreferenceSets.get(ps.getOwnerId());
                    return originalPs == null || !originalPs.equals(ps);
                })
                .toList();

        List<PreferenceSet> slowSets = new LinkedList<>();
        for (var preferenceSet : changedSets) {
            final var original = originalPreferenceSets.get(preferenceSet.getOwnerId());
            if (original == null || !dataSource.tryQuickRewrite(original, preferenceSet)) {
                slowSets.add(preferenceSet);
            }
        }

        if (!slowSets.isEmpty()) {
            Set<Long> slowSetOwnerIds = slowSets.stream()
                    .map(PreferenceSet::getOwnerId)
                    .collect(Collectors.toCollection(TreeSet::new));
            var originalSlowSets = originalPreferenceSets.values()
                    .stream()
                    .filter(ps -> slowSetOwnerIds.contains(ps.getOwnerId()))
                    .toList();
            dataSource.delete(originalSlowSets);
            dataSource.add(slowSets);
        }

        originalPreferenceSets.clear();
        preferenceSets.clear();

        log.debug("Executed {} quick and {} slow changes",
                changedSets.size() - slowSets.size(), slowSets.size());
    }
}
