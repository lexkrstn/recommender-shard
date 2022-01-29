package com.lexkrstn.recommender.shard;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class PreferenceChangeBulk {
    private final Logger log = LoggerFactory.getLogger(PreferenceChangeBulk.class);
    private final PreferenceDataSource dataSource;
    private final HashMap<Long, PreferenceSet> originalPreferenceSets = new HashMap<>();
    private final HashMap<Long, PreferenceSet> preferenceSets = new HashMap<>();

    public PreferenceChangeBulk(PreferenceDataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void addPreference(PreferenceSet preferenceSet, long entityId) {
        var foundPreferenceSet= preferenceSets.get(preferenceSet.getOwnerId());
        if (foundPreferenceSet == null) {
            originalPreferenceSets.put(preferenceSet.getOwnerId(), preferenceSet);
            foundPreferenceSet = (PreferenceSet) preferenceSet.clone();
            preferenceSets.put(preferenceSet.getOwnerId(), foundPreferenceSet);
        }
        foundPreferenceSet.add(entityId);
    }

    public void removePreference(PreferenceSet preferenceSet, long entityId) {
        var foundPreferenceSet= preferenceSets.get(preferenceSet.getOwnerId());
        if (foundPreferenceSet == null) {
            originalPreferenceSets.put(preferenceSet.getOwnerId(), preferenceSet);
            foundPreferenceSet = (PreferenceSet) preferenceSet.clone();
            preferenceSets.put(preferenceSet.getOwnerId(), foundPreferenceSet);
        }
        foundPreferenceSet.remove(entityId);
    }

    public void execute() throws IOException {
        List<PreferenceSet> changedSets = preferenceSets.values()
                .stream()
                .filter(ps -> !originalPreferenceSets.get(ps.getOwnerId()).equals(ps))
                .toList();
        List<PreferenceSet> slowSets = new LinkedList<>();
        for (var preferenceSet : changedSets) {
            if (!dataSource.tryQuickRewrite(preferenceSet)) {
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
        log.debug("Executed {} quick and {} slow changes",
                changedSets.size() - slowSets.size(), slowSets.size());
        preferenceSets.clear();
    }
}
