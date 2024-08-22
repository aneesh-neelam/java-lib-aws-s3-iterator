package me.aneeshneelam.lib.aws.s3.v2;

import reactor.core.publisher.Flux;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;


public class AsyncObjectIterator implements Iterator<S3Object> {

    private final S3AsyncClient s3AsyncClient;
    private final ListObjectsV2Request listObjectsV2Request;
    private final Duration requestTimeoutDuration;

    private CompletableFuture<ListObjectsV2Response> listObjectsV2ResponseCompletableFuture;
    private boolean listObjectsV2ResponseTruncated;
    private String listObjectsV2ResponseContinuationToken;
    private Flux<S3Object> listObjectsV2ResponseFlux;

    public AsyncObjectIterator(S3AsyncClient s3AsyncClient,
                               ListObjectsV2Request listObjectsV2Request,
                               Duration requestTimeoutDuration) {
        this.requestTimeoutDuration = requestTimeoutDuration;
        this.s3AsyncClient = s3AsyncClient;
        this.listObjectsV2Request = listObjectsV2Request;

        this.listObjectsV2ResponseCompletableFuture = CompletableFuture.completedFuture(null);
        this.listObjectsV2ResponseTruncated = false;
        this.listObjectsV2ResponseContinuationToken = null;
        this.listObjectsV2ResponseFlux = null;
    }

    private void checkListObjectsV2ResponseState() {
        if (this.listObjectsV2ResponseFlux != null) {
            if (this.listObjectsV2ResponseTruncated) {
                ListObjectsV2Request listObjectsV2ContinuationRequest = this.listObjectsV2Request.toBuilder()
                        .continuationToken(this.listObjectsV2ResponseContinuationToken)
                        .build();

                this.listObjectsV2ResponseCompletableFuture = this.s3AsyncClient.listObjectsV2(listObjectsV2ContinuationRequest);
                this.listObjectsV2ResponseCompletableFuture.thenAccept(this::setListObjectsV2ResponsePaginationState);
            }
        } else {
            this.listObjectsV2ResponseCompletableFuture = this.s3AsyncClient.listObjectsV2(this.listObjectsV2Request);
            this.listObjectsV2ResponseCompletableFuture.thenAccept(this::setListObjectsV2ResponsePaginationState);
        }
    }

    private void setListObjectsV2ResponsePaginationState(ListObjectsV2Response listObjectsV2Response) {
        this.listObjectsV2ResponseTruncated = listObjectsV2Response.isTruncated();
        this.listObjectsV2ResponseContinuationToken = listObjectsV2Response.nextContinuationToken();
        this.listObjectsV2ResponseFlux = Flux.fromIterable(listObjectsV2Response.contents());
    }

    @Override
    public boolean hasNext() {
        this.checkListObjectsV2ResponseState();
        return this.listObjectsV2ResponseFlux.next()
                .hasElement()
                .blockOptional(this.requestTimeoutDuration)
                .isPresent();
    }

    @Override
    public S3Object next() {
        this.checkListObjectsV2ResponseState();
        return this.listObjectsV2ResponseFlux.next()
                .block(this.requestTimeoutDuration);
    }
}
