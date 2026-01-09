import com.tmax.tibero.hibernate.tool.schema.extract.internal.SequenceInformationExtractorTiberoDatabaseImpl;

import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.tool.schema.extract.internal.SequenceInformationExtractorLegacyImpl;
import org.hibernate.tool.schema.extract.spi.SequenceInformationExtractor;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.ResultSet;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * TiberoDialectSequenceInformationExtractorTest
 *
 * 목적:
 *  - dialect.getSequenceInformationExtractor()가 Tibero INSTANCE를 반환하는지 검증
 *  - all_sequences에서 extractor가 사용하는 컬럼(min_value/max_value/increment_by)이
 *    실제로 존재하고 숫자로 읽히는지 검증
 */
public class SequenceInformationExtractorTest extends AbstractTiberoDialectTestBase {

    private SessionFactoryImplementor sfi;
    private Dialect dialect;
    private SequenceInformationExtractor extractor;

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

        extractor = dialect.getSequenceInformationExtractor();
        assertNotNull(extractor);
    }

    @Test
    public void testSequenceInformationExtractorInstance() {
        assertSame("Dialect should return Tibero extractor singleton",
                SequenceInformationExtractorTiberoDatabaseImpl.INSTANCE,
                extractor);
    }

    /**
     * protected 메서드를 테스트 가능하도록 public wrapper 제공
     */
    static class TestableExtractor extends SequenceInformationExtractorTiberoDatabaseImpl {
        public String catalogColumn() { return sequenceCatalogColumn(); }
        public String schemaColumn() { return sequenceSchemaColumn(); }
        public String startValueColumn() { return sequenceStartValueColumn(); }
        public String minValueColumn() { return sequenceMinValueColumn(); }
        public String maxValueColumn() { return sequenceMaxValueColumn(); }
        public String incrementColumn() { return sequenceIncrementColumn(); }

        public Number minValue(ResultSet rs) throws Exception { return resultSetMinValue(rs); }
        public Number maxValue(ResultSet rs) throws Exception { return resultSetMaxValue(rs); }
        public Number incrementValue(ResultSet rs) throws Exception { return resultSetIncrementValue(rs); }
    }

    @Test
    public void testExtractorColumnsExistAndReadableFromAllSequences() {
        final TestableExtractor testableExtractor = new TestableExtractor();
        String catalogColumn = testableExtractor.catalogColumn();
        String schemaColumn = testableExtractor.schemaColumn();
        String startValueColumn = testableExtractor.startValueColumn();
        String minValueColumn = testableExtractor.minValueColumn();
        String maxValueColumn = testableExtractor.maxValueColumn();
        String incrementColumn = testableExtractor.incrementColumn();

        assertNull(catalogColumn);
        assertNull(schemaColumn);
        assertNull(startValueColumn);

        assertEquals("min_value", minValueColumn);
        assertEquals("max_value", maxValueColumn);
        assertEquals("increment_by", incrementColumn);

        final String seqName = uniqueObjectName("SEQ_META");

        try {
            // create sequence
            inTransaction(session ->
                    session.createNativeMutationQuery("create sequence " + seqName + " start with 5 increment by 2")
                            .executeUpdate()
            );

            // all_sequences row에서 extractor가 읽는 컬럼을 직접 조회해 숫자 변환이 가능한지 검증
            inTransaction(session -> {
                Object[] row = (Object[]) session.createNativeQuery(
                        "select " + minValueColumn + ", " + maxValueColumn + ", " + incrementColumn + " " +
                                "from all_sequences " +
                                "where upper(sequence_name) = upper('" + seqName + "')"
                ).getSingleResult();

                assertNotNull(row);
                assertEquals(3, row.length);

                // Tibero에서는 보통 BigDecimal로 반환될 가능성이 높음
                assertNotNull("min_value must exist", row[0]);
                assertNotNull("max_value must exist", row[1]);
                assertNotNull("increment_by must exist", row[2]);

                BigDecimal min = (BigDecimal) row[0];
                BigDecimal max = (BigDecimal) row[1];
                BigDecimal inc = (BigDecimal) row[2];

                assertEquals("increment_by should be 2", 2, inc.intValue());
                assertTrue("min_value should be <= start value", min.intValue() <= 5);
                assertTrue("max_value should be >= start value", max.intValue() >= 5);

                System.out.println("min=" + min + ", max=" + max + ", inc=" + inc);
            });

        } finally {
            dropSequenceWithRetry(seqName);
        }
    }

    @Test
    public void testResultSetValueExtraction_usesBigDecimalColumns() throws Exception {
        TestableExtractor extractor = new TestableExtractor();

        ResultSet rs = mock(ResultSet.class);

        when(rs.getBigDecimal(extractor.minValueColumn())).thenReturn(new BigDecimal("1"));
        when(rs.getBigDecimal(extractor.maxValueColumn())).thenReturn(new BigDecimal("999"));
        when(rs.getBigDecimal(extractor.incrementColumn())).thenReturn(new BigDecimal("5"));

        Number min = extractor.minValue(rs);
        Number max = extractor.maxValue(rs);
        Number inc = extractor.incrementValue(rs);

        assertEquals(new BigDecimal("1"), min);
        assertEquals(new BigDecimal("999"), max);
        assertEquals(new BigDecimal("5"), inc);

        verify(rs).getBigDecimal(extractor.minValueColumn());
        verify(rs).getBigDecimal(extractor.maxValueColumn());
        verify(rs).getBigDecimal(extractor.incrementColumn());
    }
}

