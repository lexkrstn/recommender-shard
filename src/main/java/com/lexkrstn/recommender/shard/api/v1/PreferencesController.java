package com.lexkrstn.recommender.shard.api.v1;

import com.lexkrstn.recommender.shard.RecommenderThread;
import com.lexkrstn.recommender.shard.errors.InternalServerError;
import com.lexkrstn.recommender.shard.errors.NotFoundException;
import com.lexkrstn.recommender.shard.models.Preference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.concurrent.ExecutionException;

@RestController()
@RequestMapping("api/v1/owners/{ownerId}")
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
    public ResponseEntity<Object> addPreference(@PathVariable Long ownerId,
                                                @PathVariable Long entityId) {
        try {
            var preference = new Preference(ownerId, entityId);
            final boolean hasAdded = recommenderThread.addPreference(preference).get();
            return ResponseEntity
                    .status(hasAdded ? HttpStatus.CREATED : HttpStatus.OK)
                    .build();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to add preference", e);
            final var internalEx = new InternalServerError(e.getMessage());
            internalEx.initCause(e);
            throw internalEx;
        }
    }

    @DeleteMapping("/preferences/{entityId}")
    public ResponseEntity<Object> deletePreference(@PathVariable Long ownerId,
                                                   @PathVariable Long entityId) {
        try {
            var preference = new Preference(ownerId, entityId);
            final boolean hasAffected = recommenderThread.removePreference(preference).get();
            return ResponseEntity
                    .status(hasAffected ? HttpStatus.OK : HttpStatus.NOT_FOUND)
                    .build();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to delete preference", e);
            final var internalEx = new InternalServerError(e.getMessage());
            internalEx.initCause(e);
            throw internalEx;
        }
    }
}
