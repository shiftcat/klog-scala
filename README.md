# Kafka streams application

이 애플리케이션은 이벤트 로그 수집 분석, 통계 처리를 위한 카프카 스트림즈 애플리케이션이다.

JVM 17에 스칼라 2.13으로 개발되었으며 카프카 스트림즈 클라이언트 3.1.0 버전을 사용하였다.


## Build

```shell
make assembly
```

## Run

* Funnel
```shell
make funnel
```

* Bucket
```shell
make bucket
```

