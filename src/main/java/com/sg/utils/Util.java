package com.sg.utils;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailService;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder;
import com.amazonaws.services.simpleemail.model.*;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagement;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterRequest;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterResult;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;

public class Util implements Serializable {

    String toEmail;
    String fromEmail;
    String subject;
    String body;
    private static Logger log = LogManager.getLogger(Util.class);

    public Util(String toEmail, String fromEmail, String subject, String body) {
        this.toEmail = toEmail;
        this.fromEmail = fromEmail;
        this.subject = subject;
        this.body = body;

    }

    public Util() {

    }

    private ClientConfiguration cc = new ClientConfiguration()
            .withMaxConnections(Runtime.getRuntime().availableProcessors() - 1)
            .withMaxErrorRetry(10)
            .withConnectionTimeout(10000)
            .withSocketTimeout(10000)
            .withTcpKeepAlive(true);

    public void sendEMail() {
        AmazonSimpleEmailService client =
                AmazonSimpleEmailServiceClientBuilder.standard()
                        .withRegion(Regions.US_WEST_2)
                        .withCredentials(new DefaultAWSCredentialsProviderChain())
                        .build();
        SendEmailRequest request = new SendEmailRequest()
                .withDestination(
                        new Destination().withToAddresses(Arrays.asList(toEmail.split(","))))
                .withMessage(new Message()
                        .withBody(new Body()
                                .withText(new Content()
                                        .withCharset("UTF-8").withData(body)))
                        .withSubject(new Content()
                                .withCharset("UTF-8").withData(subject)))
                .withSource(fromEmail);
        client.sendEmail(request);
        log.info("Email sent to:" + toEmail);
    }

    public ByteArrayOutputStream writestreamGZIP(String inputdata) throws IOException {

        ByteArrayOutputStream out = new ByteArrayOutputStream(50000000);
        try (GZIPOutputStream gzip = new GZIPOutputStream(out);
             BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(gzip, "UTF-8"), 1024)) {
            bw.write(inputdata);
        } catch (Exception e) {
            System.out.println(e.toString());

        }
        return out;
    }

    public ByteArrayOutputStream writestream(String inputdata) throws IOException {

        ByteArrayOutputStream out = new ByteArrayOutputStream(50000000);
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"), 1024)) {
            bw.write(inputdata);
        } catch (Exception e) {
            System.out.println(e.toString());

        }
        return out;
    }

    public String lineReplace(String input) {
        return new String(input.trim().replaceAll("[\r\n]+", " "));
    }

    public String getSSMParam(String key) {
        AWSSimpleSystemsManagement cli = AWSSimpleSystemsManagementClientBuilder
                .defaultClient();
        GetParameterRequest req = new GetParameterRequest();
        req.withWithDecryption(true).withName(key);
        GetParameterResult result = cli.getParameter(req);
        return result.getParameter().getValue();
    }

    public InputStream getS3Obj(String bucket, String key) {

        InputStream is = null;
        AmazonS3 cli = AmazonS3ClientBuilder
                .standard()
                .withRegion(Regions.US_WEST_2)
                .withClientConfiguration(cc)
                .withCredentials(new DefaultAWSCredentialsProviderChain()).build();

        for (S3ObjectSummary x : S3Objects.withPrefix(cli, bucket, key)) {
            is = cli.getObject(new GetObjectRequest(bucket, x.getKey())).getObjectContent();
        }

        return is;
    }

    public void putS3Obj(String bucket, String key, InputStream content) {

        AmazonS3 cli = AmazonS3ClientBuilder
                .standard()
                .withRegion(Regions.US_WEST_2)
                .withClientConfiguration(cc)
                .withCredentials(new DefaultAWSCredentialsProviderChain()).build();

        try (ByteArrayInputStream bis = new ByteArrayInputStream(IOUtils.toByteArray(content))) {
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
            objectMetadata.setContentLength(bis.available());
            PutObjectRequest por = new PutObjectRequest(bucket, key, bis, objectMetadata);
            cli.putObject(por);
            content.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }
}
