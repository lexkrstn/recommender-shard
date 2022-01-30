package com.lexkrstn.recommender.shard.models;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.*;

// TODO: SimilarityTablePool (Weak references)
/**
 * Processes PreferenceSet's to build the list of recommendations for a
 * specified PreferenceSet.
 */
public class SimilarityTable {
    /**
     * Represents the row of the table.
     */
    @Data
    @AllArgsConstructor
    public static class Row {
        private PreferenceSet preferenceSet;
        private float similarity;
    }

    /**
     * The preference set to build the table for.
     */
    private final PreferenceSet preferenceSet;

    /**
     * The rows of the table sorted by similarity in descending order.
     */
    private final ArrayList<Row> rows;

    /**
     * The maximum number of rows in the table.
     */
    private final int maxRows;

    public SimilarityTable(PreferenceSet preferenceSet, int maxRows) {
        this.preferenceSet = preferenceSet;
        this.maxRows = maxRows;
        rows = new ArrayList<>(maxRows);
    }

    /**
     * Creates a map the whose keys represent entity IDs and whose values represent weights.
     */
    public Map<Long, Float> getRecommendationMap() {
        HashMap<Long, Float> map = new HashMap<>();
        for (var row : rows) {
            for (var entityId : row.preferenceSet.getEntityIds()) {
                if (map.containsKey(entityId)) {
                    map.put(entityId, map.get(entityId) + row.getSimilarity());
                } else {
                    map.put(entityId, row.getSimilarity());
                }
            }
        }
        return map;
    }

    /**
     * Creates recommendation list sorted by weight in descending order.
     */
    public List<Recommendation> getRecommendationList() {
        var map = getRecommendationMap();
        // TODO: RecommendationListPool
        var list = new ArrayList<Recommendation>(map.size());
        for (var entry : map.entrySet()) {
            list.add(new Recommendation(entry.getKey(), entry.getValue()));
        }
        list.sort(Comparator.comparing(Recommendation::getWeight).reversed());
        return list;
    }

    /**
     * Processes a preference set and decides whether it should be stored in the
     * table. If the decision is positive adds the preference set.
     */
    public void process(PreferenceSet preferenceSet) {
        float similarity = preferenceSet.getSimilarityWith(this.preferenceSet);
        if (rows.size() < maxRows) {
            rows.add(findInsertPosition(similarity), new Row(preferenceSet, similarity));
        } else if (similarity > rows.get(rows.size() - 1).getSimilarity()) {
            rows.remove(rows.size() - 1);
            rows.add(findInsertPosition(similarity), new Row(preferenceSet, similarity));
        }

    }

    /**
     * Returns an index at which a row with the specified similarity must be
     * inserted in order to keep the rows sorted.
     */
    private int findInsertPosition(float similarity) {
        for (int i = 0; i < rows.size(); i++) {
            if (similarity > rows.get(i).getSimilarity()) {
                return i;
            }
        }
        return rows.size();
    }
}
