package com.handson.twitter.controllers;

import com.handson.twitter.services.AppTwitterStream;
import com.handson.twitter.services.AppKafkaSender;
import com.handson.twitter.services.AppKafkaReceiver;
import com.handson.twitter.nlp.SentimentAnalyzer;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import twitter4j.TwitterException;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.function.Function;

import static com.handson.twitter.configs.KafkaEmbeddedConfig.TEST_TOPIC;
import static com.handson.twitter.services.AppKafkaReceiver.FINISH_PROCESSING;

@RestController
@RequestMapping(path = "/twitter")
public class AppController {
    public static final int STOP_DELAY = 120000;
    AppTwitterStream twitter;

    @Autowired
    private AppKafkaSender kafkaSender;

    AppKafkaReceiver kafka = null;
    @RequestMapping(path = "/hello", method = RequestMethod.GET)
    public  @ResponseBody Mono<String> hello()  {
        System.out.println(">>> [Hello] <<<");
        return Mono.just("Hello");
    }

//    @RequestMapping(path = "/start", method = RequestMethod.GET)
//    public  @ResponseBody Flux<String> startAnalysis(@RequestParam(defaultValue = "world") String keyWord)
//            throws TwitterException {
//        return startAnalysis(keyWord, "ungrouped", 1, true);
//    }

    @RequestMapping(path = "/start", method = RequestMethod.GET)
    public  @ResponseBody Flux<String> startAnalysis(@RequestParam(defaultValue = "world") String keyWord,
                                               @RequestParam(defaultValue = "ungrouped") String viewMode,
                                               @RequestParam(defaultValue = "1") Integer timeWindowSec,
                                               @RequestParam(defaultValue = "true") boolean isAnalyseSentiment) throws TwitterException {
        System.out.println(">>> [twitter] <<<");
        SentimentAnalyzer analyzer = new SentimentAnalyzer();
        handleStopIfNeeded();
        if (kafka != null) {
            kafka.stopListen();
        }
        AppTwitterStream twitterStream =  new AppTwitterStream();
        kafka = new AppKafkaReceiver();
        this.twitter = twitterStream;
        twitterStream.filter(keyWord).map((x)-> kafkaSender.send(x, TEST_TOPIC)).subscribe();

        if(isAnalyseSentiment){
            return kafka.listen(TEST_TOPIC).map(x-> new TimeAndMessage(DateTime.now(), x))
                    .window(Duration.ofSeconds(timeWindowSec))
                    .flatMap(window->toArrayList(window))
                    .map(getArrayListStringFunction(analyzer));
        }else if (viewMode.equals("grouped")){
            return kafka.listen(TEST_TOPIC).map(x-> new TimeAndMessage(DateTime.now(), x))
                    .window(Duration.ofSeconds(timeWindowSec))
                    .flatMap(window->toArrayList(window))
                    .map(y->{
                        if (y.size() == 0) return "size: 0 <br>";
                        return y.get(0).cur.toString() + "size: " + y.size() + "<br>";
                    });
        }else {
            return  kafka.listen(TEST_TOPIC);
        }
    }

    private Function<ArrayList<TimeAndMessage>, String> getArrayListStringFunction(SentimentAnalyzer analyzer) {
        return items->{
            double avg = items.stream().map(x-> analyzer.analyze(x.message))
                    .mapToDouble(y->y).average().orElse(0.0);
            if (items.size() == 0) return "EMPTY<br>";
            return items.get(0).cur.toString() + "->" +  items.size() + " messages, sentiment = " + avg +  "<br>";
        };
    }

    @RequestMapping(path = "/stop", method = RequestMethod.GET)
    public Mono<String> stopAnalysis(){
        twitter.shutdown();
        kafkaSender.send(FINISH_PROCESSING, TEST_TOPIC);

        return Mono.just("ok");
    }

    public static <T> Mono<ArrayList<T>> toArrayList(Flux<T> source) {
        return  source.reduce(new ArrayList(), (a, b) -> { a.add(b);return a; });
    }

    private void handleStopIfNeeded() {
        scheduleStreamStop();
        if (kafka != null) kafka.stopListen();
        if (this.twitter != null) this.twitter.shutdown();
    }

    private void scheduleStreamStop() {
        Timer t = new Timer();
        StopTask stop = new StopTask(this);
        t.schedule(stop, STOP_DELAY);
    }

    static class StopTask extends TimerTask {
        AppController controller;
        public StopTask(AppController controller) {
            this.controller = controller;
        }
        public void run() {
            controller.stopAnalysis();
        }
    }

    static class TimeAndMessage {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd, HH:mm:ss, z");
        DateTime cur;
        String message;

        public TimeAndMessage(DateTime cur, String message) {
            this.cur = cur;
            this.message = message;
        }
    }
}
