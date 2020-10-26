package com.handson.twitter.services;


import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import twitter4j.*;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

public class AppTwitterStream implements StatusListener {

    EmitterProcessor<String> emitterProcessor;
    public TwitterStream twitterStream;

    public AppTwitterStream() {
        ConfigurationBuilder configBuilder = new ConfigurationBuilder();
        configBuilder.setDebugEnabled(true);
        configBuilder.setOAuthConsumerKey("0ybPhDyGyIMkXVNsTNJh3PDve");
        configBuilder.setOAuthConsumerSecret("u73QTeH7uUAyeI0D6jew3dQ5k7N0pweaAf4fLYDAXGD3uRnGhE");
        configBuilder.setOAuthAccessToken("902505417510715393-S3i0p6oQS3rtRL7GV05rims7kgLzSYY");
        configBuilder.setOAuthAccessTokenSecret("8SUqDCahQXLcgapFMgqNv5bZR0gWz3ng8T8xg7pOPYoh9");

        twitterStream = new TwitterStreamFactory(configBuilder.build()).getInstance();
    }

    public Flux<String> filter(String key) {
        FilterQuery filterQuery = new FilterQuery();
        String[] keys = {key};
        filterQuery.track(keys);
        twitterStream.addListener(this);
        twitterStream.filter(filterQuery);
        emitterProcessor = EmitterProcessor.create();
        emitterProcessor.map(x->x);

        return emitterProcessor;
    }

    public void shutdown() {
        this.twitterStream.shutdown();
        emitterProcessor.onComplete();
    }

    @Override
    public void onStatus(Status status) {
        emitterProcessor.onNext(status.getText());
        System.out.println(">>> [Twitter STATUS]" + status);;
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) { }

    @Override
    public void onTrackLimitationNotice(int i) { }

    @Override
    public void onScrubGeo(long l, long l1) { }

    @Override
    public void onStallWarning(StallWarning stallWarning) { }

    @Override
    public void onException(Exception e) { }
}
