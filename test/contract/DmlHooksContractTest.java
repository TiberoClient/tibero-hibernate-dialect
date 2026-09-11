package contract;

import com.tmax.tibero.hibernate.dialect.TiberoDialect;
import org.hibernate.dialect.DmlTargetColumnQualifierSupport;
import org.hibernate.dialect.unique.UniqueDelegate;
import org.hibernate.sql.ast.spi.SqlAppender;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * DML / DDL 훅 계약 (DB 없는 유닛)
 */
public class DmlHooksContractTest {

    private final TiberoDialect dialect = new TiberoDialect();

    @Test
    public void uniqueDelegate_and_dmlQualifier() {
        UniqueDelegate ud = dialect.getUniqueDelegate();
        assertNotNull(ud);
        assertEquals(DmlTargetColumnQualifierSupport.TABLE_ALIAS, dialect.getDmlTargetColumnQualifierSupport());
    }

    @Test
    public void constraintDisableEnable_statements() {
        assertEquals(
                "alter table t disable constraint fk",
                dialect.getDisableConstraintStatement("t", "fk"));
        assertEquals(
                "alter table t enable constraint fk",
                dialect.getEnableConstraintStatement("t", "fk"));
    }

    @Test
    public void appendBinaryLiteral_usesHextoraw() {
        StringBuilder sb = new StringBuilder();
        SqlAppender appender = fragment -> sb.append(fragment);
        dialect.appendBinaryLiteral(appender, new byte[]{0x0a, 0x1b});
        assertEquals("hextoraw('0a1b')", sb.toString());
    }

    @Test
    public void appendDatetimeFormat_nonEmpty() {
        StringBuilder sb = new StringBuilder();
        SqlAppender appender = fragment -> sb.append(fragment);
        dialect.appendDatetimeFormat(appender, "yyyy-MM-dd");
        assertFalse(sb.toString().isEmpty());
    }

    @Test
    public void sqlAstTranslatorFactory_isPresent() {
        assertNotNull(dialect.getSqlAstTranslatorFactory());
    }
}
