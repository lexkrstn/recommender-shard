package com.lexkrstn.recommender.shard.api;

import com.lexkrstn.recommender.shard.errors.InternalServerError;
import com.lexkrstn.recommender.shard.errors.NotFoundException;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

@ControllerAdvice
class CustomRestExceptionHandler {
    @Data
    @AllArgsConstructor
    private static class ErrorDto {
        private String code;
        private String message;
    }

    @ResponseBody
    @ExceptionHandler(NotFoundException.class)
    protected ResponseEntity<Object> handleNotFoundException(NotFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(new ErrorDto("notFound", ex.getMessage()));
    }

    @ResponseBody
    @ExceptionHandler(InternalServerError.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    protected ResponseEntity<Object> handleInternalServerError(InternalServerError ex) {
        return ResponseEntity.status(404)
                .body(new ErrorDto("internalServerError", ex.getMessage()));
    }
}
