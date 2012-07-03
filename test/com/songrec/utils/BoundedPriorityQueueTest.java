package com.songrec.utils;

import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;

public class BoundedPriorityQueueTest {
    @Test
    public void shouldBeAbleToCreateABoundedPriorityQueue(){
        BoundedPriorityQueue<Long> queue = new BoundedPriorityQueue<Long>(10);
        Assert.assertEquals(0, queue.size());
    }

    @Test
    public void shouldBeAbleAddElements(){
        BoundedPriorityQueue<Integer> queue = new BoundedPriorityQueue<Integer>(10);
        queue.add(10);
        queue.add(3);
        Assert.assertEquals(2, queue.size());
    }

    @Test
    public void headOfTheQueueShouldBeTheLeastElement(){
        BoundedPriorityQueue<Integer> queue = new BoundedPriorityQueue<Integer>(10);
        queue.add(10);
        queue.add(60);
        queue.add(20);
        queue.add(90);
        Assert.assertEquals((Integer) 10, queue.peek());
    }

    @Test
    public void shouldMaintainSizeToBeTheGivenSize(){
        BoundedPriorityQueue<Integer> queue = new BoundedPriorityQueue<Integer>(4);
        queue.add(10);
        queue.add(60);
        queue.add(20);
        queue.add(90);
        queue.add(100);
        Assert.assertEquals(4, queue.size());
        Assert.assertEquals((Integer) 20, queue.peek());
    }

    @Test
    public void shouldBeAbleToCreateABoundedQueueWithAComparator(){
        BoundedPriorityQueue<Foo> queue = new BoundedPriorityQueue<Foo>(4, new Comparator<Foo>(){
            @Override
            public int compare(Foo one, Foo another) {
                return one.getY() > another.getY() ? 1 : (one.getY() < another.getY() ? -1 : 0);
            }
        });
        
        queue.add(new Foo("a", 50));
        queue.add(new Foo("c", 10));
        queue.add(new Foo("r", 55));
        queue.add(new Foo("z", 100));
        queue.add(new Foo("y", 11));
        queue.add(new Foo("q", 80));

        Assert.assertEquals(4, queue.size());
        Assert.assertEquals(new Foo("a", 50), queue.poll());
        Assert.assertEquals(new Foo("r", 55), queue.poll());
        Assert.assertEquals(new Foo("q", 80), queue.poll());
        Assert.assertEquals(new Foo("z", 100), queue.poll());
        System.out.println(HashingX.hash("XFWERV123134"));
    }


    class Foo {
        private String x;
        private int y;

        Foo(String x, int y) {
            this.x = x;
            this.y = y;
        }

        public int getY() {
            return y;
        }

        public String getX() {
            return x;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Foo foo = (Foo) o;

            if (y != foo.y) return false;
            if (!x.equals(foo.x)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = x.hashCode();
            result = 31 * result + y;
            return result;
        }
    }
}
