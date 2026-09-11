package contract;

import com.tmax.tibero.hibernate.dialect.TiberoDialect;
import jakarta.persistence.TemporalType;
import org.hibernate.dialect.RowLockStrategy;
import org.hibernate.dialect.TimeZoneSupport;
import org.hibernate.query.sqm.CastType;
import org.hibernate.query.sqm.FetchClauseType;
import org.hibernate.query.sqm.IntervalType;
import org.hibernate.query.sqm.TemporalUnit;
import org.junit.Test;

import java.sql.Types;

import static org.junit.Assert.*;

/**
 * P1 Dialect 계약 (DB 없는 유닛)
 */
public class FeatureFlagsContractTest {

    private final TiberoDialect dialect = new TiberoDialect();

    @Test
    public void supportsFlags_matchTiberoCapability() {
        assertTrue(dialect.supportsFetchClause(FetchClauseType.ROWS_ONLY));
        assertFalse(dialect.supportsFetchClause(FetchClauseType.ROWS_WITH_TIES));
        assertFalse(dialect.supportsFetchClause(FetchClauseType.PERCENT_ONLY));
        assertFalse(dialect.supportsFetchClause(FetchClauseType.PERCENT_WITH_TIES));
        assertTrue(dialect.supportsOffsetInSubquery());
        assertTrue(dialect.supportsWindowFunctions());
        assertTrue(dialect.supportsPartitionBy());
        assertTrue(dialect.supportsRecursiveCTE());
        assertFalse(dialect.supportsLateral());
        assertTrue(dialect.supportsInsertReturningGeneratedKeys());
        assertTrue(dialect.supportsFromClauseInUpdate());
        assertFalse(dialect.supportsValuesList());
        assertTrue(dialect.supportsAlterColumnType());
        assertTrue(dialect.supportsIfExistsBeforeTableName());
        assertTrue(dialect.canDisableConstraints());
        assertEquals(RowLockStrategy.COLUMN, dialect.getWriteRowLockStrategy());
        assertEquals(TimeZoneSupport.NATIVE, dialect.getTimeZoneSupport());
        assertFalse(dialect.supportsTemporalLiteralOffset());
        assertEquals(" generated always as (a+1)", dialect.generatedAs("a+1"));
        assertEquals("modify c varchar2(10)", dialect.getAlterColumnTypeString("c", "varchar2(10)", null));
        assertNotNull(dialect.getUniqueDelegate());
        assertEquals(
                "alter table t disable constraint fk",
                dialect.getDisableConstraintStatement("t", "fk"));
    }

    @Test
    public void currentTemporalPatterns() {
        assertEquals("current_date", dialect.currentDate());
        assertEquals("current_timestamp", dialect.currentTime());
        assertEquals("current_timestamp", dialect.currentTimestamp());
        assertEquals("localtimestamp", dialect.currentLocalTime());
        assertEquals("localtimestamp", dialect.currentLocalTimestamp());
        assertEquals("current_timestamp", dialect.currentTimestampWithTimeZone());
        assertEquals(1_000_000_000L, dialect.getFractionalSecondPrecisionInNanos());
    }

    @Test
    public void castExtractTimestampPatterns_smoke() {
        assertEquals("to_char(?1,'YYYY-MM-DD')", dialect.castPattern(CastType.DATE, CastType.STRING));
        assertEquals("to_date(?1,'YYYY-MM-DD')", dialect.castPattern(CastType.STRING, CastType.DATE));
        assertEquals("to_number(to_char(?2,'DD'))", dialect.extractPattern(TemporalUnit.DAY_OF_MONTH));
        // month 계열은 add_months 기반이며 대상 타입에 따라 갈린다.
        // DATE 는 시각이 없으니 그대로, TIMESTAMP 는 add_months 가 잘라낸 소수부를 되더한다.
        String monthOnDate = dialect.timestampaddPattern(
                TemporalUnit.MONTH, TemporalType.DATE, IntervalType.SECOND);
        assertEquals("add_months(?3,?2)", monthOnDate);

        String monthOnTs = dialect.timestampaddPattern(
                TemporalUnit.MONTH, TemporalType.TIMESTAMP, IntervalType.SECOND);
        assertTrue("TIMESTAMP 는 소수부 복원식이 붙어야 함: " + monthOnTs,
                monthOnTs.contains("add_months") && monthOnTs.contains("cast(?3 as date)"));
        assertFalse("trunc 기반 옛 식으로 되돌아가면 시각이 깎임: " + monthOnTs,
                monthOnTs.contains("numtoyminterval"));

        assertTrue(dialect.timestampaddPattern(TemporalUnit.QUARTER, TemporalType.DATE, IntervalType.SECOND)
                .contains("(?2)*3"));
        assertTrue(dialect.timestampaddPattern(TemporalUnit.YEAR, TemporalType.DATE, IntervalType.SECOND)
                .contains("(?2)*12"));

        assertTrue(dialect.timestampdiffPattern(TemporalUnit.DAY, TemporalType.DATE, TemporalType.DATE)
                .contains("?3-?2"));
    }

    @Test
    public void preferredBooleanSqlType_isBit() {
        assertEquals(Types.BIT, dialect.getPreferredSqlTypeCodeForBoolean());
    }

    @Test
    public void sqlAstTranslatorFactory_isPresent() {
        assertNotNull(dialect.getSqlAstTranslatorFactory());
    }
}
