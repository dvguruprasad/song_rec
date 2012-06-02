package com.songrec.algorithms;

import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class PearsonCorrelationSimilarityTest {
    @Test
    public void testShouldFindSimilarityScoreBetweenTwoItems() {
        ArrayList<Float> first = new ArrayList<Float>();
        first.add(2.5f);
        first.add(3.5f);
        first.add(3.0f);
        first.add(3.5f);
        first.add(2.5f);
        first.add(3.0f);

        ArrayList<Float> second = new ArrayList<Float>();
        second.add(3.0f);
        second.add(3.5f);
        second.add(1.5f);
        second.add(5.0f);
        second.add(3.5f);
        second.add(3.0f);

        double score = PearsonCorrelationSimilarity.score(first, second);
        Assert.assertEquals(0.39605901719066977, score);
    }
}
