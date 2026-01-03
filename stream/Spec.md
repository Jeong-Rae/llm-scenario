# Spec: Stream 재구독(backlog+tail) 레이스로 인한 토큰 누락 검증 및 버퍼 해결 검증 (TS + Vitest)

## 1. 목적

이 테스트 스위트는 다음 두 사실을 검증한다.

1. **버퍼 없는(backlog 먼저 → tail 구독) 구현은** 스트림 재구독 또는 초기 구독 시점에서
   backlog 리플레이와 tail 구독 사이의 레이스 윈도우로 인해 **토큰(이벤트)이 누락될 수 있다**.

2. **버퍼 있는(tail 먼저 구독 + cutoff + buffer flush) 구현은** 동일 조건에서
   backlog/tail 경계 구간에서 발생 가능한 누락을 제거하여 **토큰이 누락되지 않는다**.

본 테스트는 “발생 가능성”을 확인하는 **확률적(비결정론) 테스트**로 설계한다.
즉, 훅/게이트 같은 결정론적 스케줄 고정 장치를 사용하지 않고, `sleep/jitter`만으로 경합을 유도한다.

---

## 2. 범위 / 비범위

### 범위

* Node 환경에서 동작하는 “in-memory append-only event log + observe(pubsub)”를 기반으로
* `subscribe(key, afterCursor)`가 생산한 이벤트를 수집하여
* 원본 텍스트를 토큰 단위로 append했을 때, 구독자가 재구독을 수행하는 동안 이벤트 누락이 발생하는지 검증한다.

### 비범위

* 실제 SSE 프로토콜(HTTP) 구현/네트워크 레벨 검증은 포함하지 않는다.
* 프레임워크(Nest/Express) 통합 테스트는 포함하지 않는다.
* LLM SDK(Vercel/TanStack/LangChain)와의 실제 통합은 포함하지 않는다.

---

## 3. 핵심 용어 정의

* **Event Log**: key별로 이벤트가 append-only로 축적되는 저장소
* **Cursor(id)**: 이벤트의 단조 증가 식별자(재구독의 기준점)
* **Backlog**: `afterCursor` 이후부터 현재까지 누적된 이벤트를 읽어 전달하는 구간
* **Tail**: 구독 이후 새로 append되는 이벤트를 실시간으로 전달하는 구간
* **레이스 윈도우**: backlog를 읽은 뒤 tail을 구독하기 전까지의 짧은 시간 구간(누락 위험)

---

## 4. 테스트 대상 구현(개념)

테스트는 동일한 저장소(EventLog)를 대상으로, 서로 다른 2개의 구독 구현을 비교한다.

### 4.1 버퍼 없는 구독(취약 구현)

* 순서: `backlog replay → tail subscribe`
* 특징: backlog 읽기와 tail 구독 사이에 append된 이벤트는 backlog에도 없고, tail에도 못 들어가 **누락 가능**

### 4.2 버퍼 있는 구독(개선 구현)

* 순서:

  1. tail을 먼저 구독하되 초기에는 내부 버퍼에 적재
  2. `cutoff = latestCursor`를 측정
  3. backlog를 `(afterCursor, cutoff]`까지만 replay
  4. 버퍼에서 `> cutoff` 이벤트를 flush
  5. 이후 실시간 tail 전달
* 특징: 경계 구간에서 들어온 이벤트를 버퍼가 보전하여 누락 방지

---

## 5. 데이터/토큰 모델

### 5.1 원본 텍스트

* 일반 로렘 문단(코드펜스/특수 패턴 제외)
* 예: `"Lorem ipsum dolor sit amet, consectetur adipiscing elit..."`

### 5.2 토큰화

* 테스트에서는 “이벤트 단위”를 단순화하기 위해 토큰을 **문자 단위** 또는 **고정 길이 chunk(예: 1~3 chars)** 로 쪼갠다.
* 이유:

  * 레이스로 인해 이벤트 하나가 빠지면 최종 문자열이 쉽게 달라져 **누락 검출이 확실해짐**
  * 문자 단위면 누락이 더 쉽게 관측됨(재현률 상승)

### 5.3 이벤트

* `event.payload = { token: string }` 형태
* 이벤트 id는 append 순서대로 증가

---

## 6. 관측/검증 기준

### 6.1 최종 텍스트 일치성 검증(메인)

* 생산자가 append한 원본 토큰을 모두 합친 `expectedText`
* 구독자가 수집한 토큰을 합친 `receivedText`
* 검증:

  * **정상**: `receivedText === expectedText`
  * **누락/손상**: `receivedText !== expectedText`

### 6.2 누락 위치 확인(보조)

* `expectedText`와 `receivedText` 간 diff를 계산해도 되지만, 최소 스펙에서는 다음만 만족하면 충분하다.

  * `receivedText.length < expectedText.length` 이면 누락 가능성이 매우 높음
  * 또는 mismatch index 탐지(첫 불일치 위치)

---

## 7. 시나리오 설계

테스트는 “재구독”을 포함해야 한다. 재구독의 의미는 다음과 같다.

* 구독자가 한 번 구독해서 일부 토큰을 수신
* 특정 시점에 구독을 끊고(네트워크 끊김 가정)
* **마지막으로 받은 cursor**를 기준으로 `afterCursor = lastSeenId`로 재구독
* 재구독 과정에서도 backlog+tail 경계 레이스가 발생할 수 있다.

### 7.1 기본 시나리오: 단일 재구독

1. Producer가 토큰 append를 비동기(jitter 포함)로 진행
2. Consumer1이 `subscribe(afterCursor=0)`으로 수신 시작
3. 랜덤 시점에 Consumer1 종료(“끊김”)
4. Consumer2가 `subscribe(afterCursor=consumer1LastSeen)`으로 재구독
5. Producer 완료 후, Consumer2도 종료
6. 두 consumer가 받은 토큰을 합쳐 `receivedText` 구성
7. `receivedText`와 `expectedText` 비교

### 7.2 멀티 재구독(확률 증폭)

* 위 과정을 2~4회 반복하여 레이스가 발생할 기회를 늘린다.
* 단, 테스트 실행 시간을 위해 반복 횟수는 default 2회 정도로 제한하고, 필요 시 옵션으로 증가시킨다.

---

## 8. 타이밍/경합 유도 전략(훅 없이 sleep만)

레이스 발생 가능성을 높이기 위해 다음 jitter를 조합한다.

### 8.1 Producer jitter

* 각 토큰 append 전에 `sleep(random(0..3ms))`
* 생산 시작도 `sleep(random(0..10ms))`로 랜덤 지연

### 8.2 Consumer jitter

* 최초 구독 시작을 `sleep(random(0..5ms))` 후 시작
* 구독을 끊는 시점을 `sleep(random(20..80ms))` 등으로 랜덤하게 선택

### 8.3 반복 수행(통계적 검증)

* 동일 시나리오를 `ITERATIONS`회 실행하여 확률적으로 누락을 관측
* 버퍼 없는 구현은 누락 카운트가 **0보다 커야 한다**(환경에 따라 ITERATIONS 조정)
* 버퍼 있는 구현은 누락 카운트가 **0이어야 한다** 또는 매우 낮아야 한다(이상적으론 0)

> 주의: 확률 테스트는 환경(부하, CPU, event loop 스케줄)에 따라 실패/성공 빈도가 달라질 수 있다.
> 따라서 `ITERATIONS`, jitter 범위, 토큰 개수(텍스트 길이)를 조절 가능하게 한다.

---

## 9. 테스트 케이스 목록

### TC1: 버퍼 없는 구독은 재구독 시 누락이 발생할 수 있다(확률적)

* Given: 버퍼 없는 subscribe 구현, 로렘 텍스트 토큰화
* When: 단일 재구독 시나리오를 N회 반복
* Then: `mismatchCount > 0` (적어도 1회 이상 누락/불일치가 관측)

### TC2: 버퍼 있는 구독은 재구독 시 누락이 발생하지 않는다

* Given: 버퍼 있는 subscribe 구현, 동일 조건
* When: 동일 시나리오를 N회 반복
* Then: `mismatchCount === 0`

### TC3(선택): 버퍼 없는 구독에서 mismatch 발생 시 길이/불일치 지점 로깅

* mismatch가 발생한 경우, 첫 mismatch index, expected/received 슬라이스를 로그로 출력(디버그 용도)
* 이 케이스는 assertion이 아니라, 실패 분석을 돕는 부가 출력

---

## 12. 성공 기준

* TC1에서 적어도 일부 반복에서 mismatch가 관측되면 “누락 가능성”이 증명된다.
* TC2에서 모든 반복에서 match가 유지되면 “버퍼 방식이 누락을 제거”함이 증명된다.
* mismatch 시 로그가 충분히 제공되어 재현 조건(시드, iteration index)을 확인 가능하면 좋다.

---

## 13. 구현 시 주의사항(테스트 목적 정렬)

* 이 테스트는 “버퍼 없는 방식이 항상 깨진다”를 증명하려는 것이 아니다.
* “레이스가 존재하므로 **깨질 수 있다**”를 관측하는 것이 목적이다.
* 반대로 “버퍼 방식은 설계상 누락이 없어야 한다”를 보여준다.
* 따라서 TC1은 **확률적 관측(>0)**, TC2는 **설계적 보장(=0)** 구조로 설계한다.