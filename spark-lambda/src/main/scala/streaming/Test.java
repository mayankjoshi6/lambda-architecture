package streaming;

public class Test {
    public static void main(String[] args) {

        //int i=0;
        int fib=1;
        int prev=0;
        int curr=0;

        while (fib<1000) {
            System.out.println(fib);
            prev=curr;
            curr=fib;
            fib=prev+curr;
        }
    System.out.println("###############################################33");
    calcFibb(1,0,0);
    }

    public static void calcFibb(int fibb, int prev,int curr) {
        System.out.println(fibb);
        if(fibb > 1000) return;
        if(fibb==1)prev=fibb;
        calcFibb(prev+curr,curr,fibb);
    }
}
