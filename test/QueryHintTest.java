import com.tmax.tibero.hibernate.dialect.TiberoDialect;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class QueryHintTest {

    private final TiberoDialect dialect = new TiberoDialect();

    // ------------------------------------------------------------------------
    // getQueryHintString(String sql, String hints)
    // ------------------------------------------------------------------------

    @Test
    public void testGetQueryHintString_select() {
        String sql = "select * from emp";
        String hinted = dialect.getQueryHintString(sql, "FULL(emp)");

        assertEquals("select /*+ FULL(emp) */ * from emp", hinted);
    }

    @Test
    public void testGetQueryHintString_insert() {
        String sql = "insert into emp(id, name) values (1, 'a')";
        String hinted = dialect.getQueryHintString(sql, "APPEND");

        assertEquals("insert /*+ APPEND */ into emp(id, name) values (1, 'a')", hinted);
    }

    @Test
    public void testGetQueryHintString_update() {
        String sql = "update emp set sal = 100 where id = 1";
        String hinted = dialect.getQueryHintString(sql, "INDEX(emp emp_idx)");

        assertEquals("update /*+ INDEX(emp emp_idx) */ emp set sal = 100 where id = 1", hinted);
    }

    @Test
    public void testGetQueryHintString_delete() {
        String sql = "delete from emp where id = 1";
        String hinted = dialect.getQueryHintString(sql, "FULL(emp)");

        assertEquals("delete /*+ FULL(emp) */ from emp where id = 1", hinted);
    }

    @Test
    public void testGetQueryHintString_withLeadingSpaces() {
        String sql = "   select * from emp";
        String hinted = dialect.getQueryHintString(sql, "FULL(emp)");

        assertEquals("   select /*+ FULL(emp) */ * from emp", hinted);
    }

    @Test
    public void testGetQueryHintString_withLeadingBlockComment() {
        String sql = "/* comment */ select * from emp";
        String hinted = dialect.getQueryHintString(sql, "FULL(emp)");

        assertEquals("/* comment */ select /*+ FULL(emp) */ * from emp", hinted);
    }

    // ------------------------------------------------------------------------
    // getQueryHintString(String query, List<String> hintList)
    // ------------------------------------------------------------------------

    @Test
    public void testGetQueryHintString_hintListEmpty_returnsOriginal() {
        String sql = "select * from emp";
        String hinted = dialect.getQueryHintString(sql, Collections.emptyList());

        assertEquals(sql, hinted);
    }

    @Test
    public void testGetQueryHintString_hintListSingle() {
        String sql = "select * from emp";
        List<String> hints = Collections.singletonList("FULL(emp)");

        String hinted = dialect.getQueryHintString(sql, hints);

        assertEquals("select /*+ FULL(emp) */ * from emp", hinted);
    }

    @Test
    public void testGetQueryHintString_hintListMultiple() {
        String sql = "select * from emp";
        List<String> hints = Arrays.asList("FULL(emp)", "INDEX(emp emp_idx)");

        String hinted = dialect.getQueryHintString(sql, hints);

        assertEquals("select /*+ FULL(emp) INDEX(emp emp_idx) */ * from emp", hinted);
    }

    @Test
    public void testGetQueryHintString_hintListBlank_shouldReturnOriginal() {
        String sql = "select * from emp";
        List<String> hints = Collections.singletonList("");

        String hinted = dialect.getQueryHintString(sql, hints);

        assertEquals(sql, hinted);
    }

    @Test
    public void testGetQueryHintString_multipleBlockComments_shouldWork() {
        String sql = "/* c1 */ /* c2 */ select * from emp";
        String hinted = dialect.getQueryHintString(sql, "FULL(emp)");

        assertEquals("/* c1 */ /* c2 */ select /*+ FULL(emp) */ * from emp", hinted);
    }

    // ------------------------------------------------------------------------
    // statementType() failure cases (based on SQL_STATEMENT_TYPE_PATTERN)
    // ------------------------------------------------------------------------

    @Test(expected = IllegalArgumentException.class)
    public void testGetQueryHintString_invalidStatementType_merge_throwsException() {
        dialect.getQueryHintString("merge into emp using dual", "FULL(emp)");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetQueryHintString_withLineCommentPrefix_throwsException() {
        // 패턴은 /* */ 주석만 허용하고 -- 주석은 허용하지 않음
        dialect.getQueryHintString("-- comment\nselect * from emp", "FULL(emp)");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetQueryHintString_statementWithoutSpaceAfterKeyword_throwsException() {
        // 패턴은 (select|insert|update|delete) 뒤에 \\s+ 요구
        dialect.getQueryHintString("select/*+hint*/ * from emp", "FULL(emp)");
    }
}

