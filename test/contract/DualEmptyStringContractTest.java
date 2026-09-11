package contract;

import com.tmax.tibero.hibernate.dialect.TiberoDialect;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * dual / empty-string Dialect 계약 (DB 없는 유닛)
 */
public class DualEmptyStringContractTest {

    private final TiberoDialect dialect = new TiberoDialect();

    @Test
    public void getDual_returnsDual() {
        assertEquals("dual", dialect.getDual());
    }

    @Test
    public void getFromDualForSelectOnly_returnsFromDual() {
        assertEquals(" from dual", dialect.getFromDualForSelectOnly());
    }

    @Test
    public void isEmptyStringTreatedAsNull_true() {
        assertTrue(dialect.isEmptyStringTreatedAsNull());
    }
}
