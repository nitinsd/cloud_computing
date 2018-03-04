package edu.bu.metcs.hw2.task2;
import java.util.Comparator;

public class ErrorRateAscendingComparator implements Comparator<MedallionErrorRateTuple> {
    public int compare(MedallionErrorRateTuple first, MedallionErrorRateTuple second) {
        if (first.getErrorRate() < second.getErrorRate())
            return -1;
        if (first.getErrorRate() > second.getErrorRate())
            return 1;
        
        return 0;
    }
}