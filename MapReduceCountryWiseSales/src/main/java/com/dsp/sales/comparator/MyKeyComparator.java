package com.dsp.sales.comparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyKeyComparator extends WritableComparator {
    protected MyKeyComparator() {
        super(Text.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        Text key1 = (Text) w1;
        Text key2 = (Text) w2;          
        return -1 * key2.compareTo(key1);
    }
}
