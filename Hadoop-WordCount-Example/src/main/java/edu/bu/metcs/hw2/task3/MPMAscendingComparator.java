package edu.bu.metcs.hw2.task3;

import java.util.Comparator;

public class MPMAscendingComparator implements Comparator<DriverMPMTuple> {
    public int compare(DriverMPMTuple first, DriverMPMTuple second) {
        if (first.getMpm() < second.getMpm())
            return -1;
        if (first.getMpm() > second.getMpm())
            return 1;
        
        return 0;
    }
}