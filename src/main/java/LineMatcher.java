/**
 * @author jirenhe | jirenhe@shulie.io
 * @since 2021/10/22 3:36 下午
 */
public class LineMatcher {

    private static final char[] pattern = "shulie_1024:".toCharArray();

    public static int match(char[] chars, int length) {
        if (length < 13) {
            return -1;
        }
        for (int i = 0; i < 12; i++) {
            if (chars[i] != pattern[i]) {
                return -1;
            }
        }
        int result = 0;
        for (int i = 12; i < length; i++) {
            int digit = Character.digit(chars[i], 10);
            result *= 10;
            result += digit;
        }
        return result;
    }

    public static void main(String[] args) {
        char[] chars = "shulie_1024:9994".toCharArray();
        System.out.println(match(chars, 16));
    }
}