package com.songrec.utils;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;

public class BoundedPriorityQueue<T> {
    PriorityQueue<T> queue;
    private int capacity;
    private Comparator<T> comparator;

    public BoundedPriorityQueue(int capacity) {
        this.capacity = capacity;
        queue = new PriorityQueue<T>(capacity);
    }

    public BoundedPriorityQueue(int capacity, Comparator<T> comparator) {
        this.capacity = capacity;
        this.comparator = comparator;
        queue = new PriorityQueue<T>(capacity, comparator);
    }

    public int size() {
        return queue.size();
    }

    public void add(T value) {
        if (size() < capacity) {
            queue.add(value);
        } else if(compare(queue.peek(), value) < 0){
            queue.add(value);
            queue.poll();
        }
    }

    private int compare(T one, T another) {
        if(null != comparator){
            return comparator.compare(one, another);
        }
        return ((Comparable<T>) one).compareTo(another);
    }

    public T peek() {
        return queue.peek();
    }

    public T poll() {
        return queue.poll();
    }

    public ArrayList<T> retrieve() {
        return Lists.newArrayList(queue);
    }
}
