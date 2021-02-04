package com.homework.mr.template.join.pojo;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 作者： 马中华：http://blog.csdn.net/zhongqi2513
 * 日期： 2016年10月11日下午 9:22:37
 * 
 * 描述： 一个POJO类
 */
public class MovieRate implements WritableComparable<MovieRate> {
	
	private String movieid;
	private String userid;
	private int rate;
	private String movieName;
	private String movieType;
	private long ts;

	public String getMovieid() {
		return movieid;
	}

	public void setMovieid(String movieid) {
		this.movieid = movieid;
	}

	public String getUserid() {
		return userid;
	}

	public void setUserid(String userid) {
		this.userid = userid;
	}

	public int getRate() {
		return rate;
	}

	public void setRate(int rate) {
		this.rate = rate;
	}

	public String getMovieName() {
		return movieName;
	}

	public void setMovieName(String movieName) {
		this.movieName = movieName;
	}

	public String getMovieType() {
		return movieType;
	}

	public void setMovieType(String movieType) {
		this.movieType = movieType;
	}

	public long getTs() {
		return ts;
	}

	public void setTs(long ts) {
		this.ts = ts;
	}

	public MovieRate() {
	}

	public void set(String movieid, String userid, int rate, String movieName, String movieType, long ts) {
		this.movieid = movieid;
		this.userid = userid;
		this.rate = rate;
		this.movieName = movieName;
		this.movieType = movieType;
		this.ts = ts;
	}

	@Override
	public String toString() {
		return movieid + "\t" + userid + "\t" + rate + "\t" + movieName + "\t" + movieType + "\t" + ts;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(movieid);
		out.writeUTF(userid);
		out.writeInt(rate);
		out.writeUTF(movieName);
		out.writeUTF(movieType);
		out.writeLong(ts);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.movieid = in.readUTF();
		this.userid = in.readUTF();
		this.rate = in.readInt();
		this.movieName = in.readUTF();
		this.movieType = in.readUTF();
		this.ts = in.readLong();
	}

	@Override
	public int compareTo(MovieRate mr) {
		int it = mr.getMovieid().compareTo(this.movieid);
		if (it == 0) {
			return mr.getUserid().compareTo(this.userid);
		} else {
			return it;
		}
	}
}
