package com.handson.twitter.nlp;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentClass;
import edu.stanford.nlp.util.CoreMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class AppSentiment {
    StanfordCoreNLP pipeline;
    Map<String, Double> sentimentValues = new HashMap<>();

    public AppSentiment() {
        Properties pipelineProps = new Properties();
        pipelineProps.put("annotators", "tokenize, ssplit, parse, sentiment");
        pipeline = new StanfordCoreNLP(pipelineProps);
        sentimentValues.put("Very negative", 1d);
        sentimentValues.put("Negative", 2d);
        sentimentValues.put("Neutral",3d);
        sentimentValues.put("Positive",4d);
        sentimentValues.put("Very positive",5d);

    }

    public Double analyze(String text) {
        Annotation annotation = pipeline.process(text);
        List<CoreMap> sentences =  annotation.get(CoreAnnotations.SentencesAnnotation.class);
        return   sentences.stream()
                .map(sentence->sentence.get(SentimentClass.class))
                .map(sentimentStr->sentimentValues.get(sentimentStr))
                .mapToDouble(x->x).sum()
                / (Double.parseDouble(String.valueOf(sentences.size())));
    }
}
