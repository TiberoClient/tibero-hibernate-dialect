package support;

import org.hibernate.Session;
import org.hibernate.testing.junit4.BaseCoreFunctionalTestCase;

import java.util.Locale;
import java.util.function.Function;

public class AbstractTiberoDialectTestBase extends BaseCoreFunctionalTestCase {

    // =========================================================================
    // Utilities
    // =========================================================================

    // 테스트간 테스트 테이블 분리
    protected String uniqueObjectName(String prefix) {
        int max = 30;

        String p = prefix.toUpperCase(Locale.ROOT);

        // suffix는 짧게: nanoTime 마지막 6자리만
        String suffix = String.valueOf(System.nanoTime() % 1_000_000);

        String candidate = p + "_" + suffix;

        if (candidate.length() > max) {
            int allowedPrefixLen = max - (suffix.length() + 1);
            p = p.substring(0, Math.max(1, allowedPrefixLen));
            candidate = p + "_" + suffix;
        }
        return candidate;
    }

    protected void safeRollback(Session session) {
        try {
            if (session.getTransaction() != null && session.getTransaction().isActive()) {
                session.getTransaction().rollback();
            }
        } catch (Exception ignored) {}
    }

    /**
     * cleanup drop 이 lock release 지연 때문에 실패할 수 있으므로 retry 를 건다.
     * 테스트 setup 에서 이전 테이블을 지울 때도 같은 이유로 한 번에 안 될 수 있다.
     *
     * <p>테이블이 없어서 나는 실패는 정상이므로 끝까지 실패해도 예외를 던지지 않는다.
     * 다만 <b>조용히 넘어가지는 않는다</b> — 지워지지 않은 채 다음 테스트가 돌면
     * 원인이 엉뚱한 곳에서 드러나므로, 마지막 실패는 이유와 함께 남긴다.
     */
    protected void dropTableWithRetry(String table) {
        final int maxRetry = 5;
        final long sleepMs = 200;

        for (int i = 1; i <= maxRetry; i++) {
            try {
                inTransaction(session -> {
                    session.createNativeMutationQuery("drop table " + table).executeUpdate();
                });
                return;
            } catch (Exception e) {

                if (i == maxRetry) {
                    Throwable root = e;
                    while (root.getCause() != null) {
                        root = root.getCause();
                    }
                    System.err.println("[dropTableWithRetry] " + table + " 를 " + maxRetry
                            + "회 시도 후에도 지우지 못했다 — " + root.getClass().getSimpleName()
                            + ": " + String.valueOf(root.getMessage()).split("\n")[0]);
                    return;
                }

                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException ignored) {}
            }
        }
    }

    protected void dropSequenceWithRetry(String seqName) {
        inTransaction(session -> {
            try {
                session.createNativeMutationQuery("drop sequence " + seqName).executeUpdate();
            } catch (Exception ignored) {}
        });
    }

    protected void ensureTableExists(String tableName, String ddl) {
        inTransaction(session -> {
            Long cnt = session.createNativeQuery(
                    "select count(*) from user_tables where table_name='" + tableName + "'",
                    Long.class
            ).getSingleResult();

            if (cnt == 0) {
                session.createNativeMutationQuery(ddl).executeUpdate();
            }
        });
    }

    protected void truncateTable(String tableName) {
        inTransaction(session -> session.createNativeMutationQuery(
                "truncate table " + tableName
        ).executeUpdate());
    }

    /**
     * inTransaction은 void 반환이라, T를 반환하는 편의 메서드
     */
    protected <T> T inTransactionReturning(Function<Session, T> work) {
        final Object[] holder = new Object[1];
        inTransaction(session -> holder[0] = work.apply(session));
        return (T) holder[0];
    }
}
