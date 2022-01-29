package com.lexkrstn.recommender.shard.api.v1;

public class NotFoundException extends RuntimeException {
    NotFoundException(String message) {
        super(message);
    }
}
