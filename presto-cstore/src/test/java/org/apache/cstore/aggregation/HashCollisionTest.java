package org.apache.cstore.aggregation;

import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

public class HashCollisionTest
{
    private static final BigDecimal groupCount = new BigDecimal(20000);
    private static final BigDecimal hashSize = new BigDecimal(32 << 10);
    private static final MathContext mathContext = new MathContext(32, RoundingMode.HALF_UP);
    private static final BigDecimal rowCount = new BigDecimal(2957363);;
    @Test
    public void testLineitemQuery()
    {
        BigDecimal noHitRate = new BigDecimal("1.0", mathContext);
        for (BigDecimal i = new BigDecimal(0); i.compareTo(groupCount) < 0; i = i.add(new BigDecimal(1))) {
            noHitRate = noHitRate.multiply(hashSize.subtract(i))
                    .divide(hashSize, mathContext);
        }
        BigDecimal hitRate = new BigDecimal(1).subtract(noHitRate);
        System.out.printf("hit rate is %.4f, estimate hit count is %d",
                hitRate.doubleValue(), rowCount.multiply(hitRate).longValue());
    }

    @Test
    public void testLineitemQueryHitCount()
    {
        BigDecimal noHitRate = new BigDecimal("1.0", mathContext);
        for (BigDecimal i = new BigDecimal(0); i.compareTo(groupCount) < 0; i = i.add(new BigDecimal(1))) {
            noHitRate = noHitRate.multiply(hashSize.subtract(i))
                    .divide(hashSize, mathContext);
        }
        BigDecimal hitRate = new BigDecimal(1).subtract(noHitRate);

        System.out.printf("hit rate is %.4f, estimate hit count is %d",
                hitRate.doubleValue(), rowCount.multiply(hitRate).longValue());
    }

    @Test
    public void testPresto(){
        BigDecimal estimate = new BigDecimal("33950.646082");
        System.out.println(estimate.divide(hashSize, mathContext).multiply(rowCount).intValue());
    }
}
