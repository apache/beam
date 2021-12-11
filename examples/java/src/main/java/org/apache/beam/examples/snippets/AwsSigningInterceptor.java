package org.apache.beam.examples.snippets;

import org.apache.http.HttpRequestInterceptor;

import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;

public class AWSSigningAuthorizationInterceptorProvider implements ElasticsearchIO.AuthorizationInterceptorProvider {

    private final String servicename;
    private final String region;
    private final String awsCredentialsProviderSerialized;

    public AWSSigningAuthorizationInterceptorProvider(String servicename, String region, AwsCredentialsProvider credentialsProvider) {
        this.awsCredentialsProviderSerialized =
                AwsSerializableUtils.serializeAwsCredentialsProvider(credentialsProvider);
        this.servicename = servicename;
        this.region = region;
    }

    @Override
    public HttpRequestInterceptor getAuthorizationRequestInterceptor() {
        AWS4Signer signer = new AWS4Signer();
        signer.setServiceName(this.servicename);
        signer.setRegionName(this.region);
        return new AWSRequestSigningApacheInterceptor(this.servicename, signer,
                AwsSerializableUtils.deserializeAwsCredentialsProvider(this.awsCredentialsProviderSerialized));
    }
}