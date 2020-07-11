package edu.cqu.kafkastreaming;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


/**
 * 处理log中的消息发送到recommender
 */
public class LogProcessor implements Processor<byte[],byte[]> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
    }

    @Override
    public void process(byte[] bytes, byte[] bytes2) {
        String input = new String(bytes2);
        //根据前缀过滤日志信息,提取后面的内容
        if(input.contains("PRODUCT_RATING_PREFIX:")){
            System.out.println("product rating coming!!!!" + input);
            input = input.split("PRODUCT_RATING_PREFIX:")[1].trim();
            context.forward("logProcessor".getBytes(),input.getBytes());
        }
    }


    @Override
    public void punctuate(long l) {
    }

    public static void main(String[] args) {
        int[] test = {1,2,3,4,5};
        System.out.println(PrintMinNumber(test));
    }

    public static String PrintMinNumber(int [] numbers) {
        Integer[] test = new Integer[numbers.length];
        System.arraycopy(numbers,0,test,0,numbers.length);
        Arrays.sort(test,(a,b) ->{
            int a_high = a,b_high = b,a_bits = 1,b_bits = 1;
            while(a_high / 10 != 0){
                a_bits += 1;
                a_high = a_high / 10;
            }
            while(b_high / 10 != 0){
                b_bits += 1;
                b_high = b_high / 10;
            }
            if(a_high != b_high) return a_high - b_high;
            if(a > b){
                int mul = a_bits - b_bits;
                while(mul-- >0){
                    b = b * 10 + a_high;
                }
            }else{
                int mul = b_bits - a_bits;
                while(mul-- >0){
                    a = a * 10 + a_high;
                }
            }
            return a - b;
        });
        StringBuilder builder = new StringBuilder();
        for(int i = 0; i < test.length; i++){
            builder.append(test[i]);
        }
        return builder.toString();
    }

    @Override
    public void close() {
    }
}
