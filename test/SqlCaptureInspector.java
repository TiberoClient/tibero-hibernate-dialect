import org.hibernate.resource.jdbc.spi.StatementInspector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SqlCaptureInspector implements StatementInspector {
    private static final List<String> SQLS = Collections.synchronizedList(new ArrayList<>());

    public static void clear() {
        SQLS.clear();
    }

    public static List<String> getSqls() {
        return new ArrayList<>(SQLS);
    }

    @Override
    public String inspect(String sql) {
        SQLS.add(sql);
        return sql;
    }
}
