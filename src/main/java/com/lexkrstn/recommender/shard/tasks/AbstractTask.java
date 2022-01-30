package com.lexkrstn.recommender.shard.tasks;

import com.lexkrstn.recommender.shard.models.PreferenceSet;

/**
 * Abstract task of the RecommenderThread.
 */
public abstract class AbstractTask {
    /**
     * The listener that executes upon the task completion.
     */
    public interface CompletionListener {
        void onCompleted();
    }

    private CompletionListener completionListener;

    /**
     * Set the listener that executes upon the task completion.
     */
    public void setCompletionListener(CompletionListener listener) {
        completionListener = listener;
    }

    /**
     * Executes the completion listener (if it's been set).
     */
    protected void complete() {
        if (completionListener != null) {
            completionListener.onCompleted();
        }
    }

    /**
     * Executes at the end of the preference set traversing cycle.
     *
     * @return A boolean value indicating whether the task must be proceeded in
     *         the next iteration. If the returned value is false the task will
     *         be removed from the queue of the taken tasks.
     */
    public boolean proceedPass() {
        complete();
        return false;
    }

    /**
     * Executes upon every iteration of the preference set traversing cycle.
     *
     * @param preferenceSet The preference set object that has been read from
     *                      the data source.
     */
    public abstract void processPreferenceSet(PreferenceSet preferenceSet);
}
