package com.tmax.tibero.hibernate.dialect.identity;

import org.hibernate.dialect.identity.IdentityColumnSupportImpl;
import org.hibernate.generator.EventType;
import org.hibernate.id.insert.GetGeneratedKeysDelegate;
import org.hibernate.persister.entity.EntityPersister;

public class TiberoIdentityColumnSupport extends IdentityColumnSupportImpl {

    public static final TiberoIdentityColumnSupport INSTANCE = new TiberoIdentityColumnSupport();

    /**
     * IDENTITY 컬럼 지원 여부
     * -- true면 이런 컬럼 생성 가능
     * CREATE TABLE users (
     *     id NUMBER GENERATED AS IDENTITY
     * );
     */
    @Override
    public boolean supportsIdentityColumns() {
        return true;
    }

    /**
     * INSERT ... SELECT 문에서 IDENTITY 값을 반환받을 수 있는지 여부
     * -- true면 INSERT 후 생성된 ID를 바로 조회 가능
     * INSERT INTO users (name) VALUES ('John') RETURNING id INTO ?
     */
    @Override
    public boolean supportsInsertSelectIdentity() {
        return false;
    }


    //DDL 생성 시 IDENTITY 컬럼 정의 구문
    @Override
    public String getIdentityColumnString(int type) {
        return "generated as identity";
    }


    /**
     * INSERT 후 생성된 IDENTITY 키를 가져오는 방식
     * - inferredKeys
     * -- false: 컬럼명 명시해서 getGeneratedKeys() 호출 - execute(sql, new String[]{"ID"})
     * -- true: 컬럼명 없이 getGeneratedKeys() 호출 - execute(sql, Statement.RETURN_GENERATED_KEYS)
     * Tibero JDBC는 컬럼명 없이 generated keys를 요청하면 PK가 아니라 ROWID를 반환할 수 있다
     * 따라서 inferredKeys=false로 컬럼명을 명시하도록 한다
     */
    @Override
    public GetGeneratedKeysDelegate buildGetGeneratedKeysDelegate(EntityPersister persister) {
        return new GetGeneratedKeysDelegate(persister, false, EventType.INSERT);
    }

    /**
     * INSERT 시 IDENTITY 컬럼에 넣을 값
     */
    @Override
    public String getIdentityInsertString() {
        return "default";
    }
}
