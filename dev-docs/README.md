# dev-docs

**dialect 를 고치는 사람을 위한 문서입니다. 배포 대상이 아닙니다.**

사용자가 볼 문서는 저장소 루트의 [README.md](../README.md) 와 [docs/](../docs) 에 있습니다.

| 문서 | 내용 |
|---|---|
| [dialect-decisions.md](dialect-decisions.md) | 어떤 Hibernate API 를 왜 구현하지 않았는지에 대한 판단 기록. `DialectDecisionContractTest` · `DialectDecisionCapabilityTest` 가 이 분류를 테스트로 고정하므로, 분류를 바꿀 때는 테스트도 함께 고쳐야 합니다 |

여기 문서는 `.gitattributes` 의 `export-ignore` 로 배포 아카이브에서 제외됩니다.
