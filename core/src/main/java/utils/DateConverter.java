package ir.mci.dwbi.bigdata.spark_job.core.utils;

public class DateConverter {
    public static String gregorian_to_jalali(int gy, int gm, int gd) {
        int days, jm, jd;
        {
            int gy2 = (gm > 2) ? (gy + 1) : gy;
            int[] g_d_m = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};
            days = 355666 + (365 * gy) + ((int) ((gy2 + 3) / 4)) - ((int) ((gy2 + 99) / 100)) + ((int) ((gy2 + 399) / 400)) + gd + g_d_m[gm - 1];
        }
        int jy = -1595 + (33 * ((int) (days / 12053)));
        days %= 12053;
        jy += 4 * ((int) (days / 1461));
        days %= 1461;
        if (days > 365) {
            jy += (int) ((days - 1) / 365);
            days = (days - 1) % 365;
        }
        if (days < 186) {
            jm = 1 + (int) (days / 31);
            jd = 1 + (days % 31);
        } else {
            jm = 7 + (int) ((days - 186) / 30);
            jd = 1 + ((days - 186) % 30);
        }
        return String.format("%d%02d%02d", jy, jm, jd);

    }

    public static String jalali_to_gregorian(int jy, int jm, int jd) {
        jy += 1595;
        int days = -355668 + (365 * jy) + (((int) (jy / 33)) * 8) + ((int) (((jy % 33) + 3) / 4)) + jd + ((jm < 7) ? (jm - 1) * 31 : ((jm - 7) * 30) + 186);
        int gy = 400 * ((int) (days / 146097));
        days %= 146097;
        if (days > 36524) {
            gy += 100 * ((int) (--days / 36524));
            days %= 36524;
            if (days >= 365)
                days++;
        }
        gy += 4 * ((int) (days / 1461));
        days %= 1461;
        if (days > 365) {
            gy += (int) ((days - 1) / 365);
            days = (days - 1) % 365;
        }
        int gm, gd = days + 1;
        {
            int[] sal_a = {0, 31, ((gy % 4 == 0 && gy % 100 != 0) || (gy % 400 == 0)) ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
            for (gm = 0; gm < 13 && gd > sal_a[gm]; gm++) gd -= sal_a[gm];
        }
        int[] gregorian = {gy, gm, gd};
        return String.format("%d%02d%02d", gy, gm, gd);
    }

    public static String smartjalali(String s) {
        try {
            if (Integer.parseInt(s) > 20000101) return toJalali(s);
            else return s;
        } catch (Exception e) {
            return "-1";
        }
    }

    public static String toJalali(String s) {
        return gregorian_to_jalali(Integer.parseInt(s.substring(0, 4)), Integer.parseInt(s.substring(4, 6)), Integer.parseInt(s.substring(6, 8)));
    }

    public static String toGeo(String s) {
        return jalali_to_gregorian(Integer.parseInt(s.substring(0, 4)), Integer.parseInt(s.substring(4, 6)), Integer.parseInt(s.substring(6, 8)));
    }

}
