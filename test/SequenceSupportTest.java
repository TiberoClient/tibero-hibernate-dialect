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
