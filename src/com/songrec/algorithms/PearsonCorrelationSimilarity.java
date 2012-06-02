package com.songrec.algorithms;

import java.util.ArrayList;

public class PearsonCorrelationSimilarity {
    public static double score(ArrayList<Float> first, ArrayList<Float> second) {
        int size = first.size();

        float sum1 = sum(first);
        float sum2 = sum(second);

        float sumSq1 = sumOfSquares(first);
        float sumSq2 = sumOfSquares(second);

        float sumOfProducts = sumOfProducts(first, second);

        float numerator = sumOfProducts - (sum1 * sum2) / size;
        double denominator = Math.sqrt(Math.abs((sumSq1 - ((sum1 * sum1) / size)) * (sumSq2 - ((sum2 * sum2) / size))));
        if(denominator == 0)
            return  0;
        return numerator / denominator;
    }

    private static float sumOfProducts(ArrayList<Float> first, ArrayList<Float> second) {
        float result = 0.0f;
        for(int index = 0; index < first.size(); index++)
            result += first.get(index) * second.get(index);
        return result;
    }

    private static float sumOfSquares(ArrayList<Float> list) {
        float result = 0.0f;
        for (float item : list)
            result += item * item;
        return result;
    }

    private static float sum(ArrayList<Float> list) {
        float result = 0.0f;
        for (float item : list)
            result += item;
        return result;
    }
}
