package capability;

import support.AbstractTiberoDialectTestBase;
import com.tmax.tibero.hibernate.dialect.sequence.TiberoSequenceSupport;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.sequence.SequenceSupport;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * TiberoDialectSequenceSupportTest
 *
 * 목적:
 *  - TiberoDialect.getSequenceSupport()가 TiberoSequenceSupport를 반환하는지 검증
 *  - TiberoSequenceSupport가 생성하는 create/drop/nextval SQL이 Tibero에서 정상 실행되는지 검증
 */
public class SequenceSupportTest extends AbstractTiberoDialectTestBase {

    private SessionFactoryImplementor sfi;
    private Dialect dialect;
    private SequenceSupport sequenceSupport;

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[] {};
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

        sequenceSupport = dialect.getSequenceSupport();
        assertNotNull(sequenceSupport);
    }

    @Test
    public void testSequenceSupportInstanceAndContracts() {
        assertTrue("Dialect should return TiberoSequenceSupport",
                sequenceSupport instanceof TiberoSequenceSupport);

        // contract
        assertEquals(" from dual", sequenceSupport.getFromDual());
        assertTrue(sequenceSupport.sometimesNeedsStartingValue());
    }

    @Test
    public void testSequenceSupport_createNextvalDrop_executesOnTibero() {
        final String seqName = uniqueObjectName("SEQ_SUP");

        try {
            // create sequence SQL (start with 1, increment 1)
            String createSql = sequenceSupport.getCreateSequenceString(seqName, 1, 1);
            assertNotNull(createSql);

            // drop SQL
            String dropSql = sequenceSupport.getDropSequenceString(seqName);
            assertNotNull(dropSql);

            // nextval SQL (NextvalSequenceSupport 제공)
            // 보통 "select seq.nextval from dual" 형태
            String nextValSql = sequenceSupport.getSequenceNextValString(seqName);
            assertNotNull(nextValSql);
            assertTrue(nextValSql.toLowerCase().contains("nextval"));

            // 1) create
            inTransaction(session -> session.createNativeMutationQuery(createSql).executeUpdate());

            // 2) nextval 조회
            Long v1 = inTransactionReturning(session ->
                    session.createNativeQuery(nextValSql, Long.class).getSingleResult()
            );

            Long v2 = inTransactionReturning(session ->
                    session.createNativeQuery(nextValSql, Long.class).getSingleResult()
            );

            assertNotNull(v1);
            assertNotNull(v2);
            assertTrue("Sequence should increment", v2 > v1);

        } finally {
            // cleanup
            dropSequenceWithRetry(seqName);
        }
    }

    /**
     * 생성 DDL 의 <b>공백이 세 분기에서 일치</b>하는지.
     *
     * <p>사소해 보이지만 실제로 문제가 됐다. 일반 경로만 {@code increment by  50} 처럼
     * 공백이 두 칸이었다. SQL 의미는 같지만, 스키마 비교 도구나 DDL 을 문자열로
     * 대조하는 마이그레이션 검증에서 <b>없는 차이가 있는 것처럼</b> 잡힌다.
     *
     * <p>{@code minvalue}/{@code maxvalue} 분기는 한 칸이었으므로, 같은 dialect 안에서
     * 초기값 부호에 따라 공백이 달라지는 상태이기도 했다.
     */
    @Test
    public void createSequenceString_hasConsistentSpacing() {
        final String[] sqls = {
                sequenceSupport.getCreateSequenceString("SEQ_A", 1, 50),      // 일반
                sequenceSupport.getCreateSequenceString("SEQ_B", -10, 1),     // minvalue 분기
                sequenceSupport.getCreateSequenceString("SEQ_C", 10, -1),     // maxvalue 분기
        };
        for (String sql : sqls) {
            assertFalse("공백이 연달아 두 칸 이상 나오면 안 됨: [" + sql + "]", sql.contains("  "));
            assertTrue("increment by 는 한 칸 간격이어야 함: [" + sql + "]",
                    sql.contains("increment by ") && !sql.contains("increment by  "));
        }
    }

    @Test
    public void testSequenceSupport_createWithNegativeInitialOrNegativeIncrement_generatesValidSql() {
        final String seqName1 = uniqueObjectName("SEQ_NEGI");
        final String seqName2 = uniqueObjectName("SEQ_NEGINC");

        try {
            // (initialValue < 0) && (incrementSize > 0)
            String sql1 = sequenceSupport.getCreateSequenceString(seqName1, -10, 1);
            assertTrue(sql1.toLowerCase().contains("minvalue -10"));
            inTransaction(session -> session.createNativeMutationQuery(sql1).executeUpdate());

            // (initialValue > 0) && (incrementSize < 0)
            String sql2 = sequenceSupport.getCreateSequenceString(seqName2, 10, -1);
            assertTrue(sql2.toLowerCase().contains("maxvalue 10"));
            inTransaction(session -> session.createNativeMutationQuery(sql2).executeUpdate());

        } finally {
            dropSequenceWithRetry(seqName1);
            dropSequenceWithRetry(seqName2);
        }
    }
}
