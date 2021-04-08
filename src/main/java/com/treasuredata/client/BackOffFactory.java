package com.treasuredata.client;

public class BackOffFactory
{
    private BackOffFactory()
    {
    }

    public static BackOff newBackoff(TDClientConfig config)
    {
        BackOff backoff;
        BackOffStrategy strategy = BackOffStrategy.fromString(config.retryStrategy);
        if (strategy == null) {
            strategy = BackOffStrategy.FullJitter;
        }
        switch (strategy) {
            case Exponential:
                backoff = new ExponentialBackOff(config.retryInitialIntervalMillis, config.retryMaxIntervalMillis, config.retryMultiplier);
                break;
            case EqualJitter:
                backoff = new EqualJitterBackOff(config.retryInitialIntervalMillis, config.retryMaxIntervalMillis, config.retryMultiplier);
                break;
            case FullJitter:
            default:
                backoff = new FullJitterBackOff(config.retryInitialIntervalMillis, config.retryMaxIntervalMillis, config.retryMultiplier);
                break;
        }
        return backoff;
    }
}
