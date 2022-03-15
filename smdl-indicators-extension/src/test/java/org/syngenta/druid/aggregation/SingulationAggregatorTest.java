package org.syngenta.druid.aggregation;

import org.syngenta.druid.utils.IndicatorUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SingulationAggregatorTest {

    public static void main(String[] args) {
        System.out.println("hai");
        Double[] dobArr = { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 100.0, 100.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 };
        List<Double> list = Arrays.asList(dobArr);
        getSingulation(list); //0.978494623655914 & 97.84946236559139
    }

    public static double getSingulation(List<Double> inputArr){
        System.out.println("start of get Singulation");
        double target_value = inputArr.get(0);
        double low_value = 0.5;
        double high_value = 1.5;

        int tot = 0;
        List<Double> values = new ArrayList<>();

        for(int i = 0; i < inputArr.size() -1; i++){
            if(i==0 && tot > 0)
                continue;
            values.add(inputArr.get(i));
            tot++;
        }

        if(tot==0)
            tot = 1;

        target_value = 100 * target_value / tot;
        low_value *= target_value;
        high_value *= target_value;
        double _low = 0;
        double _high = 0;

        for(int i = 0; i < values.size() -1; i++){
            double b = values.get(i + 1);
            double a = values.get(i);
            if((b-a) < low_value)
                _low++;
            else if ((b-a) > high_value)
                _high++;
        }

        double total = 1 - ( (_high + _low) / tot);
        System.out.println("total value is:"+total * 100);
        return total * 100;
    }
}
