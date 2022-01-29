package com.lexkrstn.recommender.shard.api.v1;

public class InternalServerError extends RuntimeException {
    public InternalServerError(String message) {
        super(message);
    }
}
