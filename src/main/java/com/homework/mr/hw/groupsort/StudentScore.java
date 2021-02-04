package com.homework.mr.hw.groupsort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author fanpengyi
 * @version 1.0
 * @date 2021/1/28.
 */
public class StudentScore implements WritableComparable<StudentScore> {

    private String subject;
    private String name;
    private double score;

    public StudentScore() {
    }

    public StudentScore(String subject, String name, double score) {
        this.subject = subject;
        this.name = name;
        this.score = score;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }


    //如果自定义了分组组件 这里就只进行排序
    @Override
    public int compareTo(StudentScore o) {
        //先按科目排序
        int subjectInt = o.getSubject().compareTo(this.subject);
        // 如果科目相同 按分数排序
        if(subjectInt == 0){
            return (int)(o.getScore() - this.score);
        }else{
            return subjectInt;
        }

    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeUTF(subject);
        out.writeUTF(name);
        out.writeDouble(score);


    }

    @Override
    public void readFields(DataInput in) throws IOException {

        this.subject = in.readUTF();
        this.name = in.readUTF();
        this.score = in.readDouble();

    }


    @Override
    public String toString() {
        return subject + '\t' +
                  name + '\t' +
                 score ;
    }
}
