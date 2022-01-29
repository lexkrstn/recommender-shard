package com.lexkrstn.recommender.shard.tasks;

import com.lexkrstn.recommender.shard.models.PreferenceSet;

/**
 * Abstract task of the RecommenderThread.
 */
public abstract class RecommenderTask {
    public interface CompletionListener {
        void onCompleted();
    }

    private CompletionListener completionListener;

    public void setCompletionListener(CompletionListener listener) {
        completionListener = listener;
    }

    protected void complete() {
        if (completionListener != null) {
            completionListener.onCompleted();
        }
    }

    public boolean proceedPass() {
        complete();
        return false;
    }

    public abstract void processPreferenceSet(PreferenceSet preferenceSet);
}
