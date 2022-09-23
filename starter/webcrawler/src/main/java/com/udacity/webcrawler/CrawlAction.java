package com.udacity.webcrawler;

import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RecursiveAction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class CrawlAction extends RecursiveAction {
    private final Map<String, Integer> counts;
    private final Set<String> visitedUrls;
    private final int maxDepth;
    private final Instant deadline;
    private final Clock clock;
    private final List<Pattern> ignoredUrls;
    private final String url;
    private final PageParserFactory parserFactory;

    public CrawlAction(Map<String, Integer> counts,
                       Set<String> visitedUrls,
                       int maxDepth,
                       Instant deadline,
                       Clock clock,
                       List<Pattern> ignoredUrls,
                       String url,
                       PageParserFactory parserFactory) {
        this.counts = counts;
        this.visitedUrls = visitedUrls;
        this.maxDepth = maxDepth;
        this.deadline = deadline;
        this.clock = clock;
        this.ignoredUrls = ignoredUrls;
        this.url = url;
        this.parserFactory = parserFactory;
    }

    @Override
    protected void compute() {
        if (maxDepth == 0 || clock.instant().isAfter(deadline)) {
            return;
        }
        for (Pattern pattern : ignoredUrls) {
            if (pattern.matcher(url).matches()) {
                return;
            }
        }
        if (visitedUrls.contains(url)) {
            return;
        }
        visitedUrls.add(url);

        PageParser.Result result = parserFactory.get(url).parse();

        for (ConcurrentHashMap.Entry<String, Integer> e : result.getWordCounts().entrySet()) {
            counts.compute(e.getKey(), (k, v) -> (v == null) ? e.getValue() : e.getValue() + v);
        }

        List<CrawlAction> subActions = result.getLinks().stream()
                .map(link -> new CrawlAction.Builder()
                    .setCounts(counts)
                    .setVisitedUrls(visitedUrls)
                    .setMaxDepth(maxDepth - 1)
                    .setDeadline(deadline)
                    .setClock(clock)
                    .setIgnoredUrls(ignoredUrls)
                    .setUrl(link)
                    .setParser(parserFactory)
                    .build())
                .collect(Collectors.toList());

        invokeAll(subActions);
    }

    public static final class Builder  {
        private Map<String, Integer> counts;
        private Set<String> visitedUrls;
        private int maxDepth;
        private Instant deadline;
        private Clock clock;
        private List<Pattern> ignoredUrls;
        private String url;
        private PageParserFactory parserFactory;

        public Builder setCounts(Map<String, Integer> counts) {
            this.counts = counts;
            return this;
        }

        public Builder setVisitedUrls(Set<String> visitedUrls) {
            this.visitedUrls = visitedUrls;
            return this;
        }

        public Builder setMaxDepth(int maxDepth) {
            this.maxDepth = maxDepth;
            return this;
        }

        public Builder setDeadline(Instant deadline) {
            this.deadline = deadline;
            return this;
        }

        public Builder setClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder setIgnoredUrls(List<Pattern> ignoredUrls) {
            this.ignoredUrls = ignoredUrls;
            return this;
        }

        public Builder setUrl(String url) {
            this.url = url;
            return this;
        }

        public Builder setParser(PageParserFactory parserFactory) {
            this.parserFactory = parserFactory;
            return this;
        }

        public CrawlAction build() {
            return new CrawlAction(counts,
                    visitedUrls,
                    maxDepth,
                    deadline,
                    clock,
                    ignoredUrls,
                    url,
                    parserFactory);
        }
    }
}
