package me.aneeshneelam.lib.aws.s3.v2;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.Iterator;


public class S3ObjectIterator implements Iterator<S3Object> {

    private final S3Client s3Client;
    private final ListObjectsV2Request listObjectsV2Request;

    private ListObjectsV2Response listObjectsV2Response;
    private Iterator<S3Object> s3ObjectIterator;

    public S3ObjectIterator(S3Client s3Client,
                            ListObjectsV2Request listObjectsV2Request) {

        this.s3Client = s3Client;
        this.listObjectsV2Request = listObjectsV2Request;
    }

    private void checkListObjectsV2ResponseState() {
        if (this.s3ObjectIterator != null) {
            if (!this.s3ObjectIterator.hasNext() && this.listObjectsV2Response.isTruncated()) {
                ListObjectsV2Request listObjectsV2ContinuationRequest = this.listObjectsV2Request.toBuilder()
                        .continuationToken(this.listObjectsV2Response.nextContinuationToken())
                        .build();

                this.listObjectsV2Response = this.s3Client.listObjectsV2(listObjectsV2ContinuationRequest);
                this.s3ObjectIterator = this.listObjectsV2Response.contents()
                        .iterator();
            }
        } else {
            this.listObjectsV2Response = this.s3Client.listObjectsV2(this.listObjectsV2Request);
            this.s3ObjectIterator = this.listObjectsV2Response.contents()
                    .iterator();
        }
    }

    @Override
    public boolean hasNext() {
        this.checkListObjectsV2ResponseState();
        return this.s3ObjectIterator.hasNext();
    }

    @Override
    public S3Object next() {
        this.checkListObjectsV2ResponseState();
        return this.s3ObjectIterator.next();
    }
}
