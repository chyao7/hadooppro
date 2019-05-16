package com.mr.ordermaxgroupcomparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupComparator extends WritableComparator{

    protected OrderGroupComparator(){
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a,WritableComparable b) {

        OrderBean aorderBean = (OrderBean) a;
        OrderBean borderBean = (OrderBean) b;

        int result;

        if(aorderBean.getOrder_id()>borderBean.getOrder_id()){
             result = 1;
        }else if (aorderBean.getOrder_id()<borderBean.getOrder_id()){
            result = -1;
        }else {
            result = 0;
        }

        return result;
    }
}
