# EquityTestClient

서버 검증용 콘솔 도구. 서버(`127.0.0.1:8080`)와 로컬 DB(`Equity`)에 붙는다.

```bash
dotnet run --project tools/EquityTestClient -- order sell 9001 TST1 100 1000   # 22바이트 주문 전송, 응답 코드/푸시 출력
dotnet run --project tools/EquityTestClient -- sql "SELECT * FROM orders"     # DB 조회
dotnet run --project tools/EquityTestClient -- ctrlc <서버 PID>                # 서버 콘솔에 Ctrl+C (Windows, 정상 종료 검증)
```

`ctrlc`는 대상 프로세스의 콘솔 전체에 CTRL_C_EVENT를 보내므로, 서버를 전용 콘솔에서 띄워야 한다
(예: PowerShell `Start-Process dotnet -ArgumentList ... -WindowStyle Hidden`).
