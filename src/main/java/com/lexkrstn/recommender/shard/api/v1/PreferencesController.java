package com.lexkrstn.recommender.shard.api.v1;

import com.lexkrstn.recommender.shard.RecommenderThread;
import com.lexkrstn.recommender.shard.tasks.Preference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.concurrent.ExecutionException;

@RestController()
@RequestMapping("api/v1/owners/{ownerId}") // , produces = "application/json"
public class PreferencesController {
    private final Logger log = LoggerFactory.getLogger(RecommenderThread.class);
    private final RecommenderThread recommenderThread;

    public PreferencesController(RecommenderThread recommenderThread) {
        this.recommenderThread = recommenderThread;
    }

    @GetMapping("/preferences")
    public List<Long> getPreferences(@PathVariable Long ownerId) {
        try {
            var entityIds = recommenderThread.getPreferences(ownerId).get();
            if (entityIds == null) {
                throw new NotFoundException("No owner");
            }
            return entityIds;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to add preference", e);
            final var internalEx = new InternalServerError(e.getMessage());
            internalEx.initCause(e);
            throw internalEx;
        }
    }

    @PutMapping("/preferences/{entityId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void addPreference(@PathVariable Long ownerId, @PathVariable Long entityId) {
        try {
            var preference = new Preference(ownerId, entityId);
            recommenderThread.addPreference(preference).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to add preference", e);
            final var internalEx = new InternalServerError(e.getMessage());
            internalEx.initCause(e);
            throw internalEx;
        }
    }

    @DeleteMapping("/preferences/{entityId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deletePreference(@PathVariable Long ownerId, @PathVariable Long entityId) {
        try {
            var preference = new Preference(ownerId, entityId);
            recommenderThread.removePreference(preference).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to delete preference", e);
            final var internalEx = new InternalServerError(e.getMessage());
            internalEx.initCause(e);
            throw internalEx;
        }
    }
}
