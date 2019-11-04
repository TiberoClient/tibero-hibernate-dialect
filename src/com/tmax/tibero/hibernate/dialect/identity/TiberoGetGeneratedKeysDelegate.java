package com.tmax.tibero.hibernate.dialect.identity;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.hibernate.HibernateException;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.identity.GetGeneratedKeysDelegate;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.id.PostInsertIdentityPersister;

public class TiberoGetGeneratedKeysDelegate extends GetGeneratedKeysDelegate {
    private String[] keyColumns = this.getPersister().getRootTableKeyColumnNames();

    public TiberoGetGeneratedKeysDelegate(PostInsertIdentityPersister persister, Dialect dialect) {
        super(persister, dialect);
        if (this.keyColumns.length > 1) {
            throw new HibernateException("Identity generator cannot be used with multi-column keys");
        }
    }

    protected PreparedStatement prepare(String insertSQL, SharedSessionContractImplementor session) throws SQLException {
        return session.getJdbcCoordinator().getStatementPreparer().prepareStatement(insertSQL, this.keyColumns);
    }
}
