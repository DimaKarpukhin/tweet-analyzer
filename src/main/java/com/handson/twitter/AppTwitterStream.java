package com.handson.twitter;


import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import twitter4j.*;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

public class AppTwitterStream implements StatusListener {

    EmitterProcessor<String> emitterProcessor;
    public TwitterStream twitterStream;

    public AppTwitterStream() throws TwitterException {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true);
        cb.setOAuthConsumerKey("0ybPhDyGyIMkXVNsTNJh3PDve");
        cb.setOAuthConsumerSecret("u73QTeH7uUAyeI0D6jew3dQ5k7N0pweaAf4fLYDAXGD3uRnGhE");
        cb.setOAuthAccessToken("902505417510715393-S3i0p6oQS3rtRL7GV05rims7kgLzSYY");
        cb.setOAuthAccessTokenSecret("8SUqDCahQXLcgapFMgqNv5bZR0gWz3ng8T8xg7pOPYoh9");
        Configuration conf = cb.build();
        twitterStream = new TwitterStreamFactory(conf).getInstance();
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

    @Override
    public void onStatus(Status status) {
        emitterProcessor.onNext(status.getText());
        //System.out.println(status);;
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

    }

    @Override
    public void onTrackLimitationNotice(int i) {

    }

    @Override
    public void onScrubGeo(long l, long l1) {

    }

    @Override
    public void onStallWarning(StallWarning stallWarning) {

    }

    @Override
    public void onException(Exception e) {

    }

    public void shutdown() {
        this.twitterStream.shutdown();
        emitterProcessor.onComplete();
    }
}
