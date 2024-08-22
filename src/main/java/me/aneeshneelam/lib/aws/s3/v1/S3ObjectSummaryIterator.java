package me.aneeshneelam.lib.aws.s3.v1;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.util.Iterator;


public class S3ObjectSummaryIterator implements Iterator<S3ObjectSummary> {

    private final AmazonS3 amazonS3;
    private final ListObjectsV2Request listObjectsV2Request;

    private ListObjectsV2Result listObjectsV2Result;
    private Iterator<S3ObjectSummary> s3ObjectSummaryIterator;

    public S3ObjectSummaryIterator(AmazonS3 amazonS3, ListObjectsV2Request listObjectsV2Request) {
        this.amazonS3 = amazonS3;
        this.listObjectsV2Request = listObjectsV2Request;
    }

    private void checkListObjectsV2ResponseState() {
        if (this.s3ObjectSummaryIterator != null) {
            if (this.listObjectsV2Result.isTruncated()) {
                ListObjectsV2Request listObjectsV2ContinuationRequest = ((ListObjectsV2Request) this.listObjectsV2Request.clone())
                        .withContinuationToken(this.listObjectsV2Result.getContinuationToken());
                this.listObjectsV2Result = this.amazonS3.listObjectsV2(listObjectsV2ContinuationRequest);
                this.s3ObjectSummaryIterator = this.listObjectsV2Result.getObjectSummaries()
                        .listIterator();
            }
        } else {
            this.listObjectsV2Result = this.amazonS3.listObjectsV2(this.listObjectsV2Request);
        }
    }

    @Override
    public boolean hasNext() {
        this.checkListObjectsV2ResponseState();
        return this.s3ObjectSummaryIterator.hasNext();
    }

    @Override
    public S3ObjectSummary next() {
        this.checkListObjectsV2ResponseState();
        return this.s3ObjectSummaryIterator.next();
    }
}
