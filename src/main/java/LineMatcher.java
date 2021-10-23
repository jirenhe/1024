/**
 * @author jirenhe | jirenhe@shulie.io
 * @since 2021/10/22 3:36 下午
 */
public class LineMatcher {

    private final String pattern = "shulie_1024:";

    public static int match(String line) {
        if (line.length() < 13) {
            return -1;
        }
        String str = line.substring(12);
        try {
            return Integer.parseInt(str);
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}