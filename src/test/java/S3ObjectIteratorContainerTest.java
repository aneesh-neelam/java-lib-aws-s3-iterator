import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import me.aneeshneelam.lib.aws.s3.v1.S3ObjectSummaryIterator;
import me.aneeshneelam.lib.aws.s3.v2.AsyncS3ObjectIterator;
import me.aneeshneelam.lib.aws.s3.v2.S3ObjectIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;


@Execution(ExecutionMode.CONCURRENT)
@Testcontainers(disabledWithoutDocker = true, parallel = true)
public class S3ObjectIteratorContainerTest {
    private static final long s3ObjectCount = 3000;
    private static final String s3BucketName = "aneesh-test-bucket";
    private static final String s3KeyPrefix = "files/";
    private static final String fileNameFormat = "file-%d.txt";
    private static final String fileContentFormat = "This is file %d";

    public static final String localStackImageName = "localstack/localstack";
    public static final String localStackImageVersion = "3.6.0";
    public static final String localStackFullImageName = String.format("%s:%s", localStackImageName, localStackImageVersion);
    public static final DockerImageName localStackDockerImageName = DockerImageName.parse(localStackFullImageName);

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(localStackDockerImageName)
            .withServices(LocalStackContainer.Service.S3);

    private static boolean createS3Bucket;
    private static boolean loadS3Objects;

    private AmazonS3 amazonS3;
    private S3Client s3Client;
    private S3AsyncClient s3AsyncClient;
    private S3AsyncClient s3AsyncCrtClient;

    @BeforeAll
    public static void beforeAll() {
        createS3Bucket = true;
        loadS3Objects = true;
    }

    @BeforeEach
    public void setUp() {
        String awsAccessKey = localStackContainer.getAccessKey();
        String awsSecretKey = localStackContainer.getSecretKey();
        String awsRegion = localStackContainer.getRegion();
        URI awsS3Uri = localStackContainer.getEndpointOverride(LocalStackContainer.Service.S3);

        this.amazonS3 = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(awsS3Uri.toString(), awsRegion))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsAccessKey, awsSecretKey)))
                .build();
        this.s3Client = S3Client.builder()
                .endpointOverride(awsS3Uri)
                .region(Region.of(awsRegion))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(awsAccessKey, awsSecretKey)))
                .build();
        this.s3AsyncClient = S3AsyncClient.builder()
                .endpointOverride(awsS3Uri)
                .region(Region.of(awsRegion))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(awsAccessKey, awsSecretKey)))
                .build();
        this.s3AsyncCrtClient = S3AsyncClient.crtBuilder()
                .endpointOverride(awsS3Uri)
                .region(Region.of(awsRegion))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(awsAccessKey, awsSecretKey)))
                .build();

        if (createS3Bucket) {
            CreateBucketResponse response = this.s3Client.createBucket(r -> r.bucket(s3BucketName));
            System.out.println("Created S3 Bucket, Response: " + response.toString());
            createS3Bucket = false;
        }

        if (loadS3Objects) {
            System.out.println("Uploading " + s3ObjectCount + " S3 Objects to Bucket: " + s3BucketName);
            LongStream.rangeClosed(1L, s3ObjectCount).forEach(i -> {
                String key = s3KeyPrefix + String.format(fileNameFormat, i);
                String content = String.format(fileContentFormat, i);
                PutObjectRequest request = PutObjectRequest.builder()
                        .bucket(s3BucketName)
                        .key(key)
                        .build();
                PutObjectResponse response = this.s3Client.putObject(request, RequestBody.fromString(content, StandardCharsets.UTF_8));
                System.out.println("Uploaded S3 Object to Bucket: " + s3BucketName + ", Key: " + key + ", Content: " + content + ", Response: " + response.toString());
            });
            loadS3Objects = false;
            System.out.println("Uploaded " + s3ObjectCount + " S3 Objects to Bucket: " + s3BucketName);
        }
    }

    @Test
    public void testS3ObjectIterator() {
        System.out.println("Testing: " + S3ObjectIterator.class.getCanonicalName());
        ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                .bucket(s3BucketName)
                .prefix(s3KeyPrefix)
                .build();
        S3ObjectIterator s3ObjectIterator = new S3ObjectIterator(this.s3Client, listObjectsV2Request);
        long count = StreamSupport.stream(Spliterators.spliteratorUnknownSize(s3ObjectIterator, Spliterator.ORDERED), false)
                .count();
        Assertions.assertEquals(s3ObjectCount, count);
    }

    @Test
    public void testS3ObjectIteratorAsync() {
        System.out.println("Testing: " + AsyncS3ObjectIterator.class.getCanonicalName());
        ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                .bucket(s3BucketName)
                .prefix(s3KeyPrefix)
                .build();
        AsyncS3ObjectIterator asyncS3ObjectIterator = new AsyncS3ObjectIterator(this.s3AsyncClient, listObjectsV2Request, Duration.ofSeconds(30));
        long count = StreamSupport.stream(Spliterators.spliteratorUnknownSize(asyncS3ObjectIterator, Spliterator.ORDERED), false)
                .count();
        Assertions.assertEquals(s3ObjectCount, count);
    }

    @Test
    public void testS3ObjectIteratorAsyncCrt() {
        System.out.println("Testing: " + AsyncS3ObjectIterator.class.getCanonicalName());
        ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                .bucket(s3BucketName)
                .prefix(s3KeyPrefix)
                .build();
        AsyncS3ObjectIterator asyncS3ObjectIterator = new AsyncS3ObjectIterator(this.s3AsyncCrtClient, listObjectsV2Request, Duration.ofSeconds(30));
        long count = StreamSupport.stream(Spliterators.spliteratorUnknownSize(asyncS3ObjectIterator, Spliterator.ORDERED), false)
                .count();
        Assertions.assertEquals(s3ObjectCount, count);
    }

    @Test
    public void testS3ObjectSummaryIterator() {
        System.out.println("Testing: " + S3ObjectSummaryIterator.class.getCanonicalName());
        com.amazonaws.services.s3.model.ListObjectsV2Request listObjectsV2Request = new com.amazonaws.services.s3.model.ListObjectsV2Request()
                .withBucketName(s3BucketName)
                .withPrefix(s3KeyPrefix);
        S3ObjectSummaryIterator s3ObjectSummaryIterator = new S3ObjectSummaryIterator(this.amazonS3, listObjectsV2Request);
        long count = StreamSupport.stream(Spliterators.spliteratorUnknownSize(s3ObjectSummaryIterator, Spliterator.ORDERED), false)
                .count();
        Assertions.assertEquals(s3ObjectCount, count);
    }
}
