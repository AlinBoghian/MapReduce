package Utilities;

public class Fibonacci {
    public static Integer Compute(int number){
        if(number == 1 || number == 0){
            return 1;
        }
        if(number < 0)
            return -1;

        int[] partialresults = new int[number+1];
        partialresults[0] = 0;
        partialresults[1] = 1;


        for(int i = 2 ; i <= number ; i++ ){
            partialresults[i] = partialresults[i-1] + partialresults[i-2];
        }
        return partialresults[number];
    }

}
