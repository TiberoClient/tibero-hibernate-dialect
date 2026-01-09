import org.hibernate.Session;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.junit.Before;
import org.junit.Test;

import java.util.Locale;
import java.util.function.Function;

import static org.junit.Assert.*;

/**
 * TiberoDialectGuidTest
 *
 * 목적:
 *  - TiberoDialect.getSelectGUIDString() contract 검증
 *  - 실제 Tibero DB에서 GUID 생성 SQL 실행 결과 검증
 */
public class GuidTest extends AbstractTiberoDialectTestBase {

    private SessionFactoryImplementor sfi;
    private Dialect dialect;

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[] {}; // native query만 사용
    }

    @Override
    protected void configure(Configuration cfg) {
        super.configure(cfg);
    }

    @Before
    public void setUp() {
        sfi = sessionFactory();
        dialect = sfi.getJdbcServices().getDialect();
        assertNotNull(dialect);
    }

    @Test
    public void testGetSelectGUIDStringAndExecution() {
        // --------------------------------------------------------------------
        // 1) Contract 검증
        // --------------------------------------------------------------------
        String sql = dialect.getSelectGUIDString();
        assertNotNull("Dialect GUID select string should not be null", sql);

        // statement string sanity check
        String lower = sql.toLowerCase(Locale.ROOT);
        assertTrue("GUID SQL should contain sys_guid()", lower.contains("sys_guid()"));
        assertTrue("GUID SQL should contain rawtohex", lower.contains("rawtohex"));

        // --------------------------------------------------------------------
        // 2) 실제 DB 실행 검증
        // --------------------------------------------------------------------
        String guid1 = inTransactionReturning(session ->
                session.createNativeQuery(sql, String.class).getSingleResult()
        );
        assertNotNull("GUID result should not be null", guid1);

        // rawtohex 결과는 보통 32자리 HEX 문자열 (128-bit)
        assertTrue("GUID should be hex string", guid1.matches("^[0-9A-Fa-f]+$"));
        assertEquals("GUID hex length should be 32", 32, guid1.length());

        // --------------------------------------------------------------------
        // 3) 유니크성 검증 (두 번 실행해서 다르면 OK)
        // --------------------------------------------------------------------
        String guid2 = inTransactionReturning(session ->
                session.createNativeQuery(sql, String.class).getSingleResult()
        );
        assertNotNull("Second GUID result should not be null", guid2);

        assertNotEquals("Two GUID results should be different", guid1, guid2);

        System.out.println("GUID1=" + guid1);
        System.out.println("GUID2=" + guid2);
    }
}

