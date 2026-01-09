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
     * cleanup drop이 lock release 지연 때문에 실패할 수 있으므로 retry 적용
     * 또한, 각 테스트 setup (테이블 생성 및 데이터 insert) 때 table drop 시도 시 제대로 동작하지 않을 가능성이 있음
     */
    protected void dropTableWithRetry(String table) {
        final int maxRetry = 5;
        final long sleepMs = 200;

        for (int i = 1; i <= maxRetry; i++) {
            try {
                inTransaction(session -> {
                    session.createNativeMutationQuery("drop table " + table).executeUpdate();
                });
//                System.out.println("[Cleanup] Dropped table: " + table);
                return;
            } catch (Exception e) {
//                System.out.println("[WARN] Drop table failed (" + i + "/" + maxRetry + "): " + table);
//                System.out.println("       reason: " + e.getMessage());

                if (i == maxRetry) {
//                    System.out.println("[ERROR] Drop table finally failed: " + table);
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
