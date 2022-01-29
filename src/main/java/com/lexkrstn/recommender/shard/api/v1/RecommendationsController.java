package com.lexkrstn.recommender.shard.api.v1;

import com.lexkrstn.recommender.shard.Recommendation;
import com.lexkrstn.recommender.shard.RecommenderThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.concurrent.ExecutionException;

@RestController()
@RequestMapping("api/v1/owners/{ownerId}")
public class RecommendationsController {
    private final Logger log = LoggerFactory.getLogger(RecommenderThread.class);
    private final RecommenderThread recommenderThread;

    public RecommendationsController(RecommenderThread recommenderThread) {
        this.recommenderThread = recommenderThread;
    }

    @GetMapping("/recommendations")
    public ResponseEntity<List<Recommendation>> getRecommendations(@PathVariable Long ownerId) {
        // TODO: HATEOAS for prev / next pages passing first / last owner id
        try {
            var recommendations = recommenderThread.recommend(ownerId).get();
            if (recommendations == null) {
                throw new NotFoundException("The owner not found");
            }
            return ResponseEntity.ok(recommendations);
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to get recommendations", e);
            final var internalEx = new InternalServerError(e.getMessage());
            internalEx.initCause(e);
            throw internalEx;
        }
    }
}
