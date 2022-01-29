package com.lexkrstn.recommender.shard;

import com.lexkrstn.recommender.shard.io.PreferenceChangeBulk;
import com.lexkrstn.recommender.shard.io.PreferenceDataSource;
import com.lexkrstn.recommender.shard.models.Preference;
import com.lexkrstn.recommender.shard.models.Recommendation;
import com.lexkrstn.recommender.shard.tasks.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * The background process that runs recommendation and preference tasks.
 */
public class RecommenderThread extends Thread {
    private final Logger log = LoggerFactory.getLogger(RecommenderThread.class);

    private final PreferenceDataSource dataSource;
    private final int maxRecommendTasks;
    private final PreferenceChangeBulk changeBulk;
    private final List<RecommenderTask> tasks = new LinkedList<>();
    private LinkedList<RecommenderTask> takenTasks = new LinkedList<>();
    private boolean shouldQuit = false;

    public RecommenderThread(PreferenceDataSource dataSource, int maxRecommendTasks) {
        this.dataSource = dataSource;
        this.changeBulk = new PreferenceChangeBulk(dataSource);
        this.maxRecommendTasks = maxRecommendTasks;
        start();
    }

    @Override
    public void run() {
        log.info("Started recommendation thread");
        try {
            while (takeTasks()) {
                while (dataSource.hasNext()) {
                    var preferenceSet = dataSource.next();
                    for (var task : takenTasks) {
                        task.processPreferenceSet(preferenceSet);
                    }
                }
                log.debug("Taken {} tasks", takenTasks.size());
                takenTasks = takenTasks.stream()
                        .filter(RecommenderTask::proceedPass)
                        .collect(Collectors.toCollection(LinkedList<RecommenderTask>::new));
                changeBulk.execute();
                dataSource.rewind();
            }
            log.info("Stopped recommendation thread");
        } catch (Throwable e) {
            log.error("Recommender thread stopped due to error", e);
        } finally {
            try {
                dataSource.close();
            } catch (Exception e) {
                log.error("Cannot close data source", e);
            }
        }
    }

    @PreDestroy
    public synchronized void quit() {
        log.info("Gracefully shutdown");
        shouldQuit = true;
        notifyAll();
    }

    private boolean takeTasks() {
        synchronized (this) {
            try {
                while (!shouldQuit && takenTasks.isEmpty() && tasks.isEmpty()) {
                    wait();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            if (shouldQuit) return false;

            long recommendTaskCount = takenTasks.stream()
                    .filter(task -> task instanceof RecommendTask)
                    .count();
            for (var iterator = tasks.iterator(); iterator.hasNext();) {
                var task = iterator.next();
                if (task instanceof RecommendTask) {
                    if (recommendTaskCount < maxRecommendTasks) {
                        recommendTaskCount++;
                        takenTasks.add(task);
                        iterator.remove();
                    }
                } else {
                    takenTasks.add(task);
                    iterator.remove();
                }
            }
        }
        return true;
    }

    /**
     * Adds a recommendation task.
     *
     * @param ownerId To whom to recommend.
     * @return Recommendation list or null if no preference set with such ownerId found.
     */
    public synchronized Future<List<Recommendation>> recommend(long ownerId) {
        CompletableFuture<List<Recommendation>> future = new CompletableFuture<>();
        var task = new RecommendTask(ownerId);
        task.setCompletionListener(() -> future.complete(task.getRecommendationList()));
        tasks.add(task);
        notifyAll();
        return future;
    }

    /**
     * Adds a preference addition task.
     *
     * @param preference The preference that encapsulates owner and entity id.
     * @return A boolean value indicating whether the new preference has been
     *         added. It's false only if the preference already exists in db.
     */
    public synchronized Future<Boolean> addPreference(Preference preference) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        var task = new PreferTask(preference, changeBulk);
        task.setCompletionListener(() -> future.complete(task.hasAdded()));
        tasks.add(task);
        notifyAll();
        return future;
    }

    /**
     * Adds a preference addition task.
     *
     * @param preference The preference that encapsulates owner and entity id.
     * @return A boolean value indicating whether the new preference has been
     *         added. It's false only if the preference already exists in db.
     */
    public synchronized Future<Boolean> removePreference(Preference preference) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        var task = new UnpreferTask(preference, changeBulk);
        task.setCompletionListener(() -> future.complete(task.hasAffected()));
        tasks.add(task);
        notifyAll();
        return future;
    }

    /**
     * Adds a preference listing task.
     *
     * @param ownerId The preference owner's id.
     * @return List of preference entity IDs or null if no preference set with
     *         such ownerId found..
     */
    public synchronized Future<List<Long>> getPreferences(long ownerId) {
        CompletableFuture<List<Long>> future = new CompletableFuture<>();
        var task = new GetPreferencesTask(ownerId);
        task.setCompletionListener(() -> {
            var preferenceSet = task.getPreferenceSet();
            List<Long> ids = preferenceSet != null
                    ? preferenceSet.getEntityIds().stream().toList()
                    : null;
            future.complete(ids);
        });
        tasks.add(task);
        notifyAll();
        return future;
    }
}
