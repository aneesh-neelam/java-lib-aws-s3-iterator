package me.aneeshneelam.lib.aws.s3.v2;

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class AsyncS3ObjectIterator implements Iterator<S3Object> {

    private final S3AsyncClient s3AsyncClient;
    private final ListObjectsV2Request listObjectsV2Request;
    private final Duration requestTimeoutDuration;

    private boolean listObjectsV2ResponseTruncated;
    private String listObjectsV2ResponseNextContinuationToken;
    private Iterator<S3Object> s3ObjectIterator;

    public AsyncS3ObjectIterator(S3AsyncClient s3AsyncClient,
                                 ListObjectsV2Request listObjectsV2Request,
                                 Duration requestTimeoutDuration) {

        this.requestTimeoutDuration = requestTimeoutDuration;
        this.s3AsyncClient = s3AsyncClient;
        this.listObjectsV2Request = listObjectsV2Request;

        this.listObjectsV2ResponseTruncated = false;
        this.listObjectsV2ResponseNextContinuationToken = null;
        this.s3ObjectIterator = null;
    }

    private CompletableFuture<Void> checkListObjectsV2ResponseState() {
        if (this.s3ObjectIterator != null) {
            if (!this.s3ObjectIterator.hasNext() && this.listObjectsV2ResponseTruncated) {
                ListObjectsV2Request listObjectsV2ContinuationRequest = this.listObjectsV2Request.toBuilder()
                        .continuationToken(this.listObjectsV2ResponseNextContinuationToken)
                        .build();

                return this.s3AsyncClient.listObjectsV2(listObjectsV2ContinuationRequest)
                        .thenAccept(this::setListObjectsV2ResponsePaginationState);
            }
            return CompletableFuture.completedFuture(null);
        } else {
            return this.s3AsyncClient.listObjectsV2(this.listObjectsV2Request)
                    .thenAccept(this::setListObjectsV2ResponsePaginationState);
        }
    }

    private void setListObjectsV2ResponsePaginationState(ListObjectsV2Response listObjectsV2Response) {
        this.listObjectsV2ResponseTruncated = listObjectsV2Response.isTruncated();
        this.listObjectsV2ResponseNextContinuationToken = listObjectsV2Response.nextContinuationToken();
        this.s3ObjectIterator = listObjectsV2Response.contents()
                .iterator();
    }

    @Override
    public boolean hasNext() {
        try {
            return this.checkListObjectsV2ResponseState()
                    .thenApply(aVoid -> this.s3ObjectIterator.hasNext())
                    .get(requestTimeoutDuration.getSeconds(), TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public S3Object next() {
        try {
            return this.checkListObjectsV2ResponseState()
                    .thenApply(aVoid -> this.s3ObjectIterator.next())
                    .get(requestTimeoutDuration.getSeconds(), TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
