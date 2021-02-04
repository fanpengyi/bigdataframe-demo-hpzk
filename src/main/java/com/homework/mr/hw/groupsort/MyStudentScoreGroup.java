package com.homework.mr.hw.groupsort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author fanpengyi
 * @version 1.0
 * @date 2021/1/28.
 *
 *
 *  大数据
 *
 */
public class MyStudentScoreGroup extends WritableComparator {




    // 如果使用 compare(WritableComparale a,WritableComparable b) 方法
    //必须告诉框架底层则在进行反序列化的时候创建的对象是谁 所以必须调用父类的构造方法告知开启反序列化和反序列化改造的对象名称

    public MyStudentScoreGroup() {
        super(StudentScore.class,true);
    }

    //决定输入到reduce的数据的分组规则

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        StudentScore stu1 = (StudentScore)a;
        StudentScore stu2 = (StudentScore)b;
        //相同学科的分到同一组
        return stu1.getSubject().compareTo(stu2.getSubject());

    }
}
