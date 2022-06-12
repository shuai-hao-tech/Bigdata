package org.geekbang.time.week1.mapreduce;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author haoshuaistart
 * @create 2022-05-11 17:50
 */
public class PhoneBean implements WritableComparable<PhoneBean> {
    private Integer upTraffic;
    private Integer downTraffic;
    private Integer totalTraffic;

    public PhoneBean() {
    }

    public PhoneBean(Integer upTraffic, Integer downTraffic) {
        this.upTraffic = upTraffic;
        this.downTraffic = downTraffic;
    }

    public Integer getUpTraffic() {
        return upTraffic;
    }

    public void setUpTraffic(Integer upTraffic) {
        this.upTraffic = upTraffic;
    }

    public Integer getDownTraffic() {
        return downTraffic;
    }

    public void setDownTraffic(Integer downTraffic) {
        this.downTraffic = downTraffic;
    }

    public Integer getTotalTraffic() {
        return totalTraffic;
    }

    public void setTotalTraffic(Integer totalTraffic) {
        this.totalTraffic = totalTraffic;
    }

    public int compareTo(PhoneBean o) {
        return this.totalTraffic - o.totalTraffic;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(upTraffic);
        dataOutput.writeInt(downTraffic);
        dataOutput.writeInt(upTraffic + downTraffic);
    }

    public void readFields(DataInput dataInput) throws IOException {
        upTraffic = dataInput.readInt();
        downTraffic = dataInput.readInt();
        totalTraffic = dataInput.readInt();
    }

//    @Override
//    public String toString() {
//        return "PhoneBean{" +
//                "upTraffic=" + upTraffic +
//                ", downTraffic=" + downTraffic +
//                ", totalTraffic=" + totalTraffic +
//                '}';
//    }


    @Override
    public String toString() {
        return  upTraffic + "  " + downTraffic + "  " + totalTraffic;
    }

}
