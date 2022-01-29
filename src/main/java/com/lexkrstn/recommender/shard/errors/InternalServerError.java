package com.lexkrstn.recommender.shard.errors;

public class InternalServerError extends RuntimeException {
    public InternalServerError(String message) {
        super(message);
    }
}
