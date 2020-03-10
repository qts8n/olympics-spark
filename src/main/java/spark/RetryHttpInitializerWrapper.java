package spark;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.http.*;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;
import com.google.common.base.Preconditions;

import java.util.logging.Logger;


/**
 * RetryHttpInitializerWrapper will automatically retry upon RPC failures, preserving the
 * auto-refresh behavior of the Google Credentials.
 *
 * @link https://github.com/GoogleCloudPlatform/java-docs-samples/
 * @link https://developers.google.com/resources/api-libraries/documentation/pubsub/v1/java/latest/com/google/api/services/pubsub/PubsubRequestInitializer.html
 */
public class RetryHttpInitializerWrapper implements HttpRequestInitializer {

    private static final Logger LOG = Logger.getLogger(RetryHttpInitializerWrapper.class.getName());
    private final Credential wrappedCredential;
    private final Sleeper sleeper;
    private static final int MILLIS_PER_MINUTE = 60 * 1000;

    /**
     * A constructor using the default Sleeper.
     *
     * @param wrappedCredential the credential used to authenticate with a Google Cloud Platform project
     */
    public RetryHttpInitializerWrapper(Credential wrappedCredential) {
        this.wrappedCredential = Preconditions.checkNotNull(wrappedCredential);
        this.sleeper = Sleeper.DEFAULT;
    }

    /**
     * Initialize an HttpRequest.
     *
     * @param request an HttpRequest that should be initialized
     */
    public void initialize(HttpRequest request) {
        request.setReadTimeout(2 * MILLIS_PER_MINUTE); // 2 minutes read timeout
        final HttpUnsuccessfulResponseHandler backoffHandler =
                new HttpBackOffUnsuccessfulResponseHandler(new ExponentialBackOff()).setSleeper(sleeper);
        request.setInterceptor(wrappedCredential);
        request.setUnsuccessfulResponseHandler(
                (request1, response, supportsRetry) -> {
                    if (wrappedCredential.handleResponse(request1, response, supportsRetry)) {
                        // If credential decides it can handle it, the return code or message indicated
                        // something specific to authentication, and no backoff is desired.
                        return true;
                    } else if (backoffHandler.handleResponse(request1, response, supportsRetry)) {
                        // Otherwise, we defer to the judgement of our internal backoff handler.
                        LOG.info("Retrying " + request1.getUrl().toString());
                        return true;
                    } else {
                        return false;
                    }
                });
        request.setIOExceptionHandler(new HttpBackOffIOExceptionHandler(new ExponentialBackOff()).setSleeper(sleeper));
    }
}