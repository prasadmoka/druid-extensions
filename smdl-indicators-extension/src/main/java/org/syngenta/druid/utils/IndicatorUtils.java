package org.syngenta.druid.utils;

import org.apache.druid.java.util.common.logger.Logger;

import java.util.ArrayList;
import java.util.List;

public class IndicatorUtils {
    private static final Logger log = new Logger(IndicatorUtils.class);
   public static double getSingulation(List<Double> doubleList){
       log.debug("start of get Singulation with list size:"+doubleList.size());
        double target_value = doubleList.get(0);
        double low_value = 0.5;
        double high_value = 1.5;

        int tot = 0;
        List<Double> values = new ArrayList<>();

        for(int i = 0; i < doubleList.size() -1; i++){
            if(i==0 && tot > 0)
                continue;
            values.add(doubleList.get(i));
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
       log.debug("total value is:"+total);
        return total * 100;
    }
}
