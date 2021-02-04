package com.homework.mr.template.udgroup;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ClazzScore implements WritableComparable<ClazzScore> {

    private String clazz;
    private String name;
    private Double score;

	public String getClazz() {
		return clazz;
	}

	public void setClazz(String clazz) {
		this.clazz = clazz;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Double getScore() {
		return score;
	}

	public void setScore(Double score) {
		this.score = score;
	}

	public ClazzScore() {
        super();
        // TODO Auto-generated constructor stub
    }

	public void set(String clazz, String name, Double score) {
		this.clazz = clazz;
		this.name = name;
		this.score = score;
	}

	@Override
    public String toString() {
        return clazz + "\t" + name + "\t" + score;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(clazz);
        out.writeUTF(name);
        out.writeDouble(score);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.clazz = in.readUTF();
        this.name = in.readUTF();
        this.score = in.readDouble();
    }

    /**
     * key排序，如果自定义了分组规则，那么当前这个方法，就只是充当排序规则使用
     */
    @Override
    public int compareTo(ClazzScore cs) {
        int it = cs.getClazz().compareTo(this.clazz);
        if (it == 0) {
            return (int) (cs.getScore() - this.score);
        } else {
            return it;
        }
    }
}