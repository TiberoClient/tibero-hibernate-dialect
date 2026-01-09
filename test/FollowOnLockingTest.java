import com.tmax.tibero.hibernate.dialect.TiberoDialect;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.query.spi.Limit;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class FollowOnLockingTest {

    private final TiberoDialect dialect = new TiberoDialect();

    private QueryOptions mockQueryOptions(boolean hasLimit, Integer firstRow) {
        QueryOptions options = mock(QueryOptions.class);

        when(options.hasLimit()).thenReturn(hasLimit);

        Limit limit = mock(Limit.class);
        when(limit.getFirstRow()).thenReturn(firstRow);
        when(options.getLimit()).thenReturn(limit);

        return options;
    }

    @Test
    public void testUseFollowOnLocking_nullSql_returnsTrue() {
        QueryOptions options = mockQueryOptions(false, null);
        assertTrue(dialect.useFollowOnLocking(null, options));
    }

    @Test
    public void testUseFollowOnLocking_emptySql_returnsTrue() {
        QueryOptions options = mockQueryOptions(false, null);
        assertTrue(dialect.useFollowOnLocking("", options));
    }

    @Test
    public void testUseFollowOnLocking_nullQueryOptions_returnsTrue() {
        assertTrue(dialect.useFollowOnLocking("select * from t", null));
    }

    @Test
    public void testUseFollowOnLocking_distinct_returnsTrue() {
        QueryOptions options = mockQueryOptions(false, null);
        assertTrue(dialect.useFollowOnLocking("select distinct id from t", options));
    }

    @Test
    public void testUseFollowOnLocking_groupBy_returnsTrue() {
        QueryOptions options = mockQueryOptions(false, null);
        assertTrue(dialect.useFollowOnLocking("select id from t group by id", options));
    }

    @Test
    public void testUseFollowOnLocking_union_returnsTrue() {
        QueryOptions options = mockQueryOptions(false, null);
        assertTrue(dialect.useFollowOnLocking("select id from t union select id from t2", options));
    }

    @Test
    public void testUseFollowOnLocking_orderByWithLimit_returnsTrue() {
        QueryOptions options = mockQueryOptions(true, null);
        assertTrue(dialect.useFollowOnLocking("select id from t order by id", options));
    }

    @Test
    public void testUseFollowOnLocking_orderByWithoutLimit_returnsFalse() {
        QueryOptions options = mockQueryOptions(false, null);
        assertFalse(dialect.useFollowOnLocking("select id from t order by id", options));
    }

    @Test
    public void testUseFollowOnLocking_offsetFirstRow_returnsTrueEvenWithoutOrderBy() {
        QueryOptions options = mockQueryOptions(true, 10);
        assertTrue(dialect.useFollowOnLocking("select id from t", options));
    }

    @Test
    public void testUseFollowOnLocking_noSpecialClauseNoLimit_returnsFalse() {
        QueryOptions options = mockQueryOptions(false, null);
        assertFalse(dialect.useFollowOnLocking("select id from t", options));
    }
}

