package com.mr.tablemr;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text,TableBean,TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        ArrayList<TableBean> tableBeans = new ArrayList<>();

        TableBean pdBean = new TableBean();

        for(TableBean value:values) {
            if (value.getFlag().equals("order")) {
                TableBean tmpBean = new TableBean();
                try {
                    BeanUtils.copyProperties(tmpBean, value);

                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                tableBeans.add(tmpBean);
            } else {
                try {
                    BeanUtils.copyProperties(pdBean, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }

            }
        }

            for(TableBean k:tableBeans){
                k.setName(pdBean.getName());
                System.out.println(k);
                context.write(k,NullWritable.get());
            }
    }
}