import java.util.Arrays;

/**
 * @author jirenhe | jirenhe@shulie.io
 * @since 2021/10/22 1:51 下午
 */
public class PrimeCheck {

    private static final boolean[] isPrime;

    static {
        isPrime = primeArray(1000000);
        isPrime[999999] = false;
    }

    private static boolean[] primeArray(int range) {
        boolean[] isPrime = new boolean[range + 1];
        isPrime[1] = false;//1不是质数
        Arrays.fill(isPrime, 2, range + 1, true);//全置为true（大于等于2的位置上）
        int n = (int)Math.sqrt(range);//对range开根号
        for (int i = 2; i <= n; i++)//注意需要小于等于n
        {
            if (isPrime[i])//查看是不是已经置false过了
            {
                for (int j = i; j * i < range; j++)//将是i倍数的位置置为false
                {isPrime[j * i] = false;}
            }
        }
        return isPrime;//返回一个boolean数组
    }

    public static boolean isPrime(int i) {
        if (i > 1000000) {
            throw new RuntimeException("OUT OF BOUND");
        }
        return isPrime[i];
    }

    public static void main(String[] args) {
        long now = System.currentTimeMillis();
        PrimeCheck.primeArray(1000000);
        System.out.println((System.currentTimeMillis() - now));
    }
}
