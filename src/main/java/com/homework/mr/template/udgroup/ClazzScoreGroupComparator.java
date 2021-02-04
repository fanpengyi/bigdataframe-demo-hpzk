package com.homework.mr.template.udgroup;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ClazzScoreGroupComparator extends WritableComparator {

    // 如果使用compare(WritableComparable a, WritableComparable b)
    // 这个方法进行分组比较，则必须告诉框架，底层在进行反序列化的时候创建的对象是谁，所以必须调用父类的构造方式告知要开启反序列化和反序列化要构造的对象名称
	ClazzScoreGroupComparator() {
        super(ClazzScore.class, true);
    }

    // 决定输入到reduce的数据的分组规则
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // TODO Auto-generated method stub
        ClazzScore cs1 = (ClazzScore) a;
        ClazzScore cs2 = (ClazzScore) b;
        int it = cs1.getClazz().compareTo(cs2.getClazz());
        return it;
    }
}